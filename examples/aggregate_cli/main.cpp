/*
 * Copyright 2018-2019 tsurugi project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <iostream>
#include <vector>

#include <glog/logging.h>
#include <boost/thread/latch.hpp>
#include <boost/thread.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/compiler.h>
#include <yugawara/binding/factory.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/step/join.h>
#include <takatori/relation/step/take_group.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/filter.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/scalar/compare.h>
#include <takatori/scalar/binary.h>
#include <takatori/scalar/variable_reference.h>
#include <takatori/plan/process.h>
#include <takatori/plan/aggregate.h>
#include <takatori/statement/execute.h>

#include <performance-tools/synchronizer.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/exchange/aggregate/step.h>
#include <jogasaki/executor/exchange/aggregate/aggregate_info.h>
#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/constants.h>
#include <jogasaki/utils/performance_tools.h>
#include <jogasaki/utils/latch_set.h>

#include <jogasaki/api/impl/result_store_channel.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <takatori/relation/step/flatten.h>
#include "params.h"
#include "producer_process.h"
#include "cli_constants.h"
#include "producer_params.h"
#include "../common/show_producer_perf_info.h"

#ifdef ENABLE_GOOGLE_PERFTOOLS
#include "gperftools/profiler.h"
#endif

DEFINE_int32(thread_pool_size, 10, "Thread pool size");  //NOLINT
DEFINE_bool(use_multithread, true, "whether using multiple threads");  //NOLINT
DEFINE_int32(downstream_partitions, 10, "Number of downstream partitions");  //NOLINT
DEFINE_int32(upstream_partitions, 10, "Number of upstream partitions");  //NOLINT
DEFINE_int32(records_per_partition, 100000, "Number of records per partition");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_int64(key_modulo, -1, "key value integer is calculated based on the given modulo. Specify -1 to disable.");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_bool(sequential_data, false, "use sequential data instead of randomly generated");  //NOLINT

namespace jogasaki::aggregate_cli {

using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::executor::exchange::aggregate;
using namespace jogasaki::scheduler;

using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::executor::exchange::forward;
using namespace jogasaki::executor::process;
using namespace jogasaki::executor::process::impl;
using namespace jogasaki::executor::process::impl::ops;
using namespace jogasaki::scheduler;

using namespace meta;
using namespace takatori::util;

namespace t = ::takatori::type;
namespace relation = ::takatori::relation;
namespace binding = ::yugawara::binding;

using ::takatori::util::fail;
using namespace ::yugawara;
using namespace ::yugawara::variable;

using kind = meta::field_type_kind;

bool fill_from_flags(
    jogasaki::aggregate_cli::params& s,
    jogasaki::configuration& cfg,
    std::string const& str = {}
) {
    gflags::FlagSaver saver{};
    if (! str.empty()) {
        if(! gflags::ReadFlagsFromString(str, "", false)) {
            std::cerr << "parsing options failed" << std::endl;
        }
    }
    cfg.single_thread(!FLAGS_use_multithread);

    s.downstream_partitions_ = FLAGS_downstream_partitions;
    s.upstream_partitions_ = FLAGS_upstream_partitions;
    s.records_per_partition_ = FLAGS_records_per_partition;
    s.debug_ = FLAGS_debug;
    s.sequential_data_ = FLAGS_sequential_data;
    s.key_modulo_ = FLAGS_key_modulo;

    cfg.core_affinity(FLAGS_core_affinity);
    cfg.initial_core(FLAGS_initial_core);
    cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);
    cfg.thread_pool_size(FLAGS_thread_pool_size);

    if (FLAGS_minimum) {
        cfg.single_thread(true);
        cfg.thread_pool_size(1);
        cfg.initial_core(1);
        cfg.core_affinity(false);

        s.upstream_partitions_ = 1;
        s.records_per_partition_ = 3;
    }

    if (cfg.assign_numa_nodes_uniformly()) {
        cfg.core_affinity(true);
    }

    std::cout << std::boolalpha <<
        "upstream_partitions:" << s.upstream_partitions_ <<
        "downstream_partitions:" << s.downstream_partitions_ <<
        " records_per_partition:" << s.records_per_partition_ <<
        " debug:" << s.debug_ <<
        " sequential:" << s.sequential_data_ <<
        " key_modulo:" << s.key_modulo_ <<
        std::endl;
    return true;
}

void dump_perf_info(bool prepare = true, bool run = true, bool completion = false) {
    auto& watch = utils::get_watch();
    if (prepare) {
        common_cli::show_producer_perf_info();
    }
    if (run) {
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_consume, time_point_consumed, "consume");
    }
    if (completion) {
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_release_pool, time_point_start_completion, "release pools");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_start_completion, time_point_end_completion, "complete");
    }
}

class cli {
public:
    // entry point from main
    int operator()(params& param, std::shared_ptr<configuration> const& cfg) {
        run(param, cfg);
        utils::get_watch().set_point(time_point_release_pool, 0);
        LOG(INFO) << "start releasing memory pool";
        (void)global::page_pool(global::pool_operation::reset);
        utils::get_watch().set_point(time_point_start_completion, 0);
        LOG(INFO) << "start completion";
        return 0;
    }

    std::shared_ptr<meta::record_meta> test_record_meta() {
        return std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                meta::field_type(meta::field_enum_tag<meta::field_type_kind::int8>),
                meta::field_type(meta::field_enum_tag<meta::field_type_kind::float8>),
            },
            boost::dynamic_bitset<std::uint64_t>{2}.flip()
        );
    }

    using aggregate_declaration = yugawara::aggregate::declaration;
    std::shared_ptr<aggregate_declaration const> find_agg_func(
        yugawara::aggregate::configurable_provider const& provider,
        std::string_view name,
        std::size_t parameter_count
    ) {
        std::shared_ptr<aggregate_declaration const> found{};
        std::function<void(std::shared_ptr<aggregate_declaration const> const&)> consumer = [&](std::shared_ptr<aggregate_declaration const> const& func){
            // assuming always found one function
            if(func->parameter_types()[0] == t::float8{}) {
                if (found) fail();
                found = func;
            }
        };
        provider.each(name, parameter_count, consumer);
        if(! found) fail();
        return found;
    }

    void create_compiled_info(
        std::shared_ptr<plan::compiler_context> const& compiler_context,
        params const& param
    ) {
        (void)param;
        auto functions = std::make_shared<yugawara::aggregate::configurable_provider>();
        executor::function::incremental::add_builtin_aggregate_functions(*functions, global::incremental_aggregate_function_repository());

        binding::factory bindings;
        auto&& g0c0 = bindings.exchange_column("g0c0");
        auto&& g0a1 = bindings.exchange_column("g0a1");
        auto&& g0c1 = bindings.exchange_column("g0c1");
        takatori::plan::graph_type p;

        auto&& g0 = p.insert(takatori::plan::aggregate {
            {
                g0c0,
            },
            {
                {
                    bindings(find_agg_func(*functions, "sum", 1)),
                    g0a1,
                    g0c1,
                }
            },
        });

        auto&& p0 = p.insert(takatori::plan::process {});
        auto g0v0 = bindings.stream_variable("g0v0");
        auto g0v1 = bindings.stream_variable("g0v1");

        auto& r0 = p0.operators().insert(relation::step::take_group {
            bindings(g0),
            {
                { g0c0, g0v0 },
                { g0c1, g0v1 },
            },
        });

        auto&& r1 = p0.operators().insert(relation::step::flatten {});
        r0.output() >> r1.input();

        auto&& r2 = p0.operators().insert(relation::emit {
            {
                { g0v0, "c0"},
                { g0v1, "c1"},
            },
        });
        r1.output() >> r2.input();

        g0.add_downstream(p0);

        auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
        vmap->bind(g0c0, t::int8{});
        vmap->bind(g0c1, t::float8{});
        vmap->bind(g0a1, t::float8{});
        vmap->bind(g0v0, t::int8{});
        vmap->bind(g0v1, t::float8{});

        yugawara::compiled_info c_info{{}, vmap};

        auto mirrors = std::make_shared<plan::mirror_container>();
        jogasaki::plan::impl::preprocess(p0, c_info, mirrors);

        compiler_context->aggregate_provider(std::move(functions));
        input_exchanges_.emplace_back(&g0);
        compiler_context->executable_statement(
            std::make_shared<plan::executable_statement>(
                std::make_shared<takatori::statement::execute>(std::move(p)),
                c_info,
                std::shared_ptr<model::statement>{},
                std::shared_ptr<variable_table_info>{},
                std::shared_ptr<variable_table>{},
                std::move(mirrors)
        )
        );
    }

    int run(params& s, std::shared_ptr<configuration> cfg) {
        auto meta = test_record_meta();
        auto compiler_context = std::make_shared<plan::compiler_context>();
        data::result_store result{};
        auto context = std::make_shared<request_context>(
            cfg,
            std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool()),
            std::shared_ptr<kvs::database>{},
            std::shared_ptr<transaction_context>{},
            nullptr,
            std::make_shared<api::impl::result_store_channel>(maybe_shared_ptr{&result})
        );
        prepare_scheduler(*context);
        create_compiled_info(compiler_context, s);

        auto& g0 = unsafe_downcast<takatori::plan::aggregate>(*input_exchanges_[0]);

        global::config_pool(cfg);
        common::graph g{};
        auto& xch = g.emplace<exchange::aggregate::step>(plan::impl::create(g0, compiler_context->executable_statement()->compiled_info()));

        auto& p = unsafe_downcast<takatori::statement::execute>(*compiler_context->executable_statement()->statement()).execution_plan();
        auto& p0 = find_process(p);

        auto& info = compiler_context->executable_statement()->compiled_info();
        auto& mirrors = compiler_context->executable_statement()->mirrors();
        auto& consumer = g.emplace<process::step>(jogasaki::plan::impl::create(p0, info, mirrors, nullptr));

        producer_params prod_params{s.records_per_partition_, s.upstream_partitions_, s.sequential_data_, s.key_modulo_};
        auto& producer = g.emplace<producer_process>(meta, prod_params);
        producer >> xch;
        xch >> consumer;

        auto map = std::make_shared<io_exchange_map>();
        map->add_input(&xch);
        consumer.io_exchange_map(std::move(map));

        consumer.did_start_task(std::make_shared<callback_type>([](callback_arg* arg){
            jogasaki::utils::get_watch().set_point(jogasaki::aggregate_cli::time_point_consume, arg->identity_);
            LOG(INFO) << arg->identity_ << " start consume";
        }));
        consumer.will_end_task(std::make_shared<callback_type>([](callback_arg* arg){
            jogasaki::utils::get_watch().set_point(jogasaki::aggregate_cli::time_point_consumed, arg->identity_);
            LOG(INFO) << arg->identity_ << " end consume";
        }));

        jogasaki::utils::get_latches().enable(sync_wait_prepare, std::min(s.upstream_partitions_, cfg->thread_pool_size()));
        consumer.partitions(s.downstream_partitions_);
        dag_controller dc{std::move(cfg)};
        dc.schedule(g, *context);
        dump_result_data(result, s);
        return 0;
    }

    void dump_result_data(data::result_store const& result, params const& param) {
        for(std::size_t i=0, n=result.partitions(); i < n; ++i) {
            LOG(INFO) << "dumping result for partition " << i;
            auto& store = result.partition(i);
            auto record_meta = store.meta();
            auto it = store.begin();
            std::size_t count = 0;
            std::size_t hash = 0;
            while(it != store.end()) {
                auto record = it.ref();
                if(param.debug_ && count < 100) {
                    std::stringstream ss{};
                    ss << record << *record_meta;
                    LOG(INFO) << ss.str();
                }
                if (count % 1000 == 0) {
                    std::stringstream ss{};
                    ss << record << *record_meta;
                    // check only 1/1000 records to save time
                    hash ^= std::hash<std::string>{}(ss.str());
                }
                ++it;
                ++count;
            }
            LOG(INFO) << "record count: " << count << " hash: " << std::hex << hash;
        }
    }
    takatori::plan::process const& find_process(takatori::plan::graph_type const& p) {
        takatori::plan::process const* p0{};
        takatori::plan::sort_from_upstream(p, [&p0](takatori::plan::step const& s){
            if (s.kind() == takatori::plan::step_kind::process) {
                p0 = &dynamic_cast<takatori::plan::process const&>(s);
            }
        });
        if (! p0) fail();
        return *p0;
    }

private:
    std::vector<maybe_shared_ptr<takatori::plan::exchange>> input_exchanges_{};

}; // class cli

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("aggregate cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("aggregate cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    jogasaki::aggregate_cli::params s{};
    auto cfg = std::make_shared<jogasaki::configuration>();
    if(! fill_from_flags(s, *cfg)) return -1;
    try {
        jogasaki::aggregate_cli::cli{}(s, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    jogasaki::utils::get_watch().set_point(jogasaki::aggregate_cli::time_point_end_completion, 0);
    LOG(INFO) << "end completion";
    jogasaki::aggregate_cli::dump_perf_info(true, true, true);

    return 0;
}
