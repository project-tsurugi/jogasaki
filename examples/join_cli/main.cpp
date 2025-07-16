/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <exception>
#include <iostream>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/exchange.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/group.h>
#include <takatori/plan/process.h>
#include <takatori/plan/step.h>
#include <takatori/plan/step_kind.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/sort_direction.h>
#include <takatori/relation/step/join.h>
#include <takatori/relation/step/take_cogroup.h>
#include <takatori/statement/execute.h>
#include <takatori/type/character.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/meta_type.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/util/maybe_shared_lock.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/api/impl/result_store_channel.h>
#include <jogasaki/callback.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/exchange/forward/source.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/executor/exchange/group/step.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/ops/default_value_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/io_exchange_map.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/model/port.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/executable_statement.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/latch_set.h>
#include <jogasaki/utils/performance_tools.h>

#include "../common/producer_constants.h"
#include "../common/show_producer_perf_info.h"
#include "cli_constants.h"
#include "params.h"
#include "producer_params.h"
#include "producer_process.h"

DEFINE_int32(thread_pool_size, 100, "Thread pool size");  //NOLINT
DEFINE_bool(use_multithread, true, "whether using multiple threads");  //NOLINT
DEFINE_int32(downstream_partitions, 10, "Number of downstream partitions");  //NOLINT
DEFINE_int32(left_upstream_partitions, 5, "Number of left upstream partitions");  //NOLINT
DEFINE_int32(right_upstream_partitions, 5, "Number of right upstream partitions");  //NOLINT
DEFINE_int32(records_per_partition, 100000, "Number of records per partition");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_int32(local_partition_default_size, 1000000, "default size for local partition used to store scan results");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_int64(key_modulo, -1, "key value integer is calculated based on the given modulo. Specify -1 to disable.");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_bool(sequential_data, false, "use sequential data instead of randomly generated");  //NOLINT
DEFINE_int32(prepare_pages, 600, "prepare specified number of memory pages per partition that are first touched beforehand. Specify -1 to disable.");  //NOLINT

namespace jogasaki::join_cli {

using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::executor::exchange::group;
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
constexpr std::size_t max_char_len = 32;

bool fill_from_flags(
    jogasaki::join_cli::params& s,
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
    s.left_upstream_partitions_ = FLAGS_left_upstream_partitions;
    s.right_upstream_partitions_ = FLAGS_right_upstream_partitions;
    s.records_per_upstream_partition_ = FLAGS_records_per_partition;
    s.debug_ = FLAGS_debug;
    s.sequential_data_ = FLAGS_sequential_data;
    s.key_modulo_ = FLAGS_key_modulo;
    s.prepare_pages_ = FLAGS_prepare_pages;

    cfg.core_affinity(FLAGS_core_affinity);
    cfg.initial_core(FLAGS_initial_core);
    cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);
    cfg.thread_pool_size(FLAGS_thread_pool_size);

    if (FLAGS_minimum) {
        cfg.single_thread(true);
        cfg.thread_pool_size(1);
        cfg.initial_core(1);
        cfg.core_affinity(false);

        s.left_upstream_partitions_ = 1;
        s.right_upstream_partitions_= 1;
        s.records_per_upstream_partition_ = 3;
    }

    if (cfg.assign_numa_nodes_uniformly()) {
        cfg.core_affinity(true);
    }

    std::cout << std::boolalpha <<
        "left_upstream_partitions:" << s.left_upstream_partitions_ <<
        "right_upstream_partitions:" << s.right_upstream_partitions_ <<
        " records_per_upstream_partition:" << s.records_per_upstream_partition_ <<
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
                meta::field_type(meta::field_enum_tag<meta::field_type_kind::int8>),
                meta::field_type(std::make_shared<meta::character_field_option>()),
            },
            boost::dynamic_bitset<std::uint64_t>{3}.flip());
    }

    void create_compiled_info(
        std::shared_ptr<plan::compiler_context> const& compiler_context,
        params const& param
    ) {
        (void)param;

        binding::factory bindings;
        auto&& g0c0 = bindings.exchange_column("g0c0");
        auto&& g0c1 = bindings.exchange_column("g0c1");
        auto&& g0c2 = bindings.exchange_column("g0c2");
        takatori::plan::graph_type p;
        auto&& g0 = p.insert(takatori::plan::group {
            {
                g0c0,
                g0c1,
                g0c2,
            },
            {
                g0c0,
                g0c1,
            },
        });
        auto&& g1c0 = bindings.exchange_column("g1c0");
        auto&& g1c1 = bindings.exchange_column("g1c1");
        auto&& g1c2 = bindings.exchange_column("g1c2");
        auto&& g1 = p.insert(takatori::plan::group {
            {
                g1c0,
                g1c1,
                g1c2,
            },
            {
                g1c0,
                g1c1,
            },
        });
        auto&& p0 = p.insert(takatori::plan::process {});
        auto g0v0 = bindings.stream_variable("g0v0");
        auto g0v1 = bindings.stream_variable("g0v1");
        auto g0v2 = bindings.stream_variable("g0v2");
        auto g1v0 = bindings.stream_variable("g1v0");
        auto g1v1 = bindings.stream_variable("g1v1");
        auto g1v2 = bindings.stream_variable("g1v2");

        auto& r0 = p0.operators().insert(relation::step::take_cogroup {
            {
                bindings(g0),
                {
                    { g0c0, g0v0 },
                    { g0c1, g0v1 },
                    { g0c2, g0v2 },
                },
            },
            {
                bindings(g1),
                {
                    { g1c0, g1v0 },
                    { g1c1, g1v1 },
                    { g1c2, g1v2 },
                },
            }
        });

        auto&& r1 = p0.operators().insert(relation::step::join {
            relation::step::join::operator_kind_type::inner
        });
        r0.output() >> r1.input();

        auto&& r2 = p0.operators().insert(relation::emit {
            {
                { g0v0, "c0"},
                { g0v1, "c1"},
                { g0v2, "c2"},
                { g1v0, "c3"},
                { g1v1, "c4"},
                { g1v2, "c5"},
            },
        });
        r1.output() >> r2.input();

        g0.add_downstream(p0);
        g1.add_downstream(p0);

        auto vmap = std::make_shared<yugawara::analyzer::variable_mapping>();
        vmap->bind(g0c0, t::int8{});
        vmap->bind(g0c1, t::int8{});
        vmap->bind(g0c2, t::character{t::varying, max_char_len});
        vmap->bind(g1c0, t::int8{});
        vmap->bind(g1c1, t::int8{});
        vmap->bind(g1c2, t::character{t::varying, max_char_len});
        vmap->bind(g0v0, t::int8{});
        vmap->bind(g0v1, t::int8{});
        vmap->bind(g0v2, t::character{t::varying, max_char_len});
        vmap->bind(g1v0, t::int8{});
        vmap->bind(g1v1, t::int8{});
        vmap->bind(g1v2, t::character{t::varying, max_char_len});

        yugawara::compiled_info c_info{{}, vmap};

        meta::variable_order order0{
            variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
            g0.columns(),
            g0.group_keys()
        };
        meta::variable_order order1{
            variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
            g1.columns(),
            g1.group_keys()
        };

        auto mirrors = std::make_shared<plan::mirror_container>();
        jogasaki::plan::impl::preprocess(p0, c_info, mirrors);

        input_exchanges_.emplace_back(&g0);
        input_exchanges_.emplace_back(&g1);
        compiler_context->executable_statement(
            std::make_shared<plan::executable_statement>(
                std::make_shared<takatori::statement::execute>(std::move(p)),
                c_info,
                std::shared_ptr<model::statement>{},
                std::shared_ptr<variable_table_info>{},
                std::shared_ptr<variable_table>{},
                std::move(mirrors),
                std::make_shared<std::string>("<sql text>")
            )
        );

    }

    int run(params& s, std::shared_ptr<configuration> cfg) {
        auto meta = test_record_meta();
        auto info = std::make_shared<group_info>(meta, std::vector<std::size_t>{0,1});

        auto compiler_context = std::make_shared<plan::compiler_context>();
        create_compiled_info(compiler_context, s);

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
        auto& g0 = unsafe_downcast<takatori::plan::group>(*input_exchanges_[0]);
        auto& g1 = unsafe_downcast<takatori::plan::group>(*input_exchanges_[1]);
        meta::variable_order input_order{
            variable_ordering_enum_tag<variable_ordering_kind::flat_record>,
            g0.columns(),
        };
        meta::variable_order order0{
            variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
            g0.columns(),
            g0.group_keys()
        };
        meta::variable_order order1{
            variable_ordering_enum_tag<variable_ordering_kind::group_from_keys>,
            g1.columns(),
            g1.group_keys()
        };

        global::config_pool(cfg);
        common::graph g{};
        producer_params l_params{s.records_per_upstream_partition_, s.left_upstream_partitions_, s.sequential_data_, s.key_modulo_, s.prepare_pages_};
        producer_params r_params{s.records_per_upstream_partition_, s.right_upstream_partitions_, s.sequential_data_, s.key_modulo_, s.prepare_pages_};
        auto& producer1 = g.emplace<producer_process>(meta, l_params);
        auto& producer2 = g.emplace<producer_process>(meta, r_params);
        auto& xch1 = g.emplace<exchange::group::step>(info, input_order, order0);
        auto& xch2 = g.emplace<exchange::group::step>(info, input_order, order1);

        auto& p = unsafe_downcast<takatori::statement::execute>(*compiler_context->executable_statement()->statement()).execution_plan();
        auto& p0 = find_process(p);

        auto& c_info = compiler_context->executable_statement()->compiled_info();
        auto& mirrors = compiler_context->executable_statement()->mirrors();
        auto& consumer = g.emplace<process::step>(jogasaki::plan::impl::create(p0, c_info, mirrors, nullptr));
        producer1 >> xch1;
        producer2 >> xch2;
        xch1 >> consumer;
        xch2 >> consumer;

        auto map = std::make_shared<io_exchange_map>();
        map->add_input(&xch1);
        map->add_input(&xch2);
        consumer.io_exchange_map(std::move(map));

        consumer.did_start_task(std::make_shared<callback_type>([](callback_arg* arg){
            jogasaki::utils::get_watch().set_point(jogasaki::join_cli::time_point_consume, arg->identity_);
            LOG(INFO) << arg->identity_ << " start consume";
        }));
        consumer.will_end_task(std::make_shared<callback_type>([](callback_arg* arg){
            jogasaki::utils::get_watch().set_point(jogasaki::join_cli::time_point_consumed, arg->identity_);
            LOG(INFO) << arg->identity_ << " end consume";
        }));
        jogasaki::utils::get_latches().enable(sync_wait_prepare,
            std::min(s.left_upstream_partitions_+s.right_upstream_partitions_, cfg->thread_pool_size()));
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
    google::InitGoogleLogging("join cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("join cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);

    jogasaki::join_cli::params s{};
    auto cfg = std::make_shared<jogasaki::configuration>();
    if(! fill_from_flags(s, *cfg)) return -1;
    try {
        jogasaki::join_cli::cli{}(s, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    jogasaki::utils::get_watch().set_point(jogasaki::join_cli::time_point_end_completion, 0);
    LOG(INFO) << "end completion";
    jogasaki::join_cli::dump_perf_info(true, true, true);
    return 0;
}

