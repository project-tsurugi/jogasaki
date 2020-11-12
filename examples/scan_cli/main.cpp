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
#include <random>

#include <glog/logging.h>
#include <boost/thread/latch.hpp>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/runtime_feature.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_options.h>
#include <yugawara/binding/factory.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <takatori/type/character.h>
#include <takatori/value/int.h>
#include <takatori/value/float.h>
#include <takatori/util/string_builder.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/emit.h>
#include <takatori/relation/scan.h>
#include <takatori/relation/filter.h>
#include <takatori/relation/project.h>
#include <takatori/statement/write.h>
#include <takatori/statement/execute.h>
#include <takatori/scalar/immediate.h>
#include <takatori/plan/process.h>
#include <takatori/serializer/json_printer.h>
#include <takatori/statement/execute.h>

#include "params.h"
#include "cli_constants.h"

#include <jogasaki/executor/process/impl/ops/take_flat.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/utils/random.h>
#include <jogasaki/utils/performance_tools.h>
#include <jogasaki/names.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/record_reader.h>
#include <jogasaki/executor/process/mock/record_writer.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>

DEFINE_int32(thread_pool_size, 3, "Thread pool size");  //NOLINT
DEFINE_bool(use_multithread, true, "whether using multiple threads");  //NOLINT
DEFINE_int32(partitions, 1, "Number of partitions");  //NOLINT
DEFINE_int32(records_per_partition, 100000, "Number of records per partition");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT
DEFINE_bool(sequential_data, false, "use sequential data instead of randomly generated");  //NOLINT

namespace jogasaki::scan_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace takatori::util;

namespace type = ::takatori::type;
namespace value = ::takatori::value;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;

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
namespace v = ::takatori::value;
namespace descriptor = ::takatori::descriptor;
namespace scalar = ::takatori::scalar;
namespace relation = ::takatori::relation;
namespace statement = ::takatori::statement;
//namespace plan = ::takatori::plan;
namespace binding = ::yugawara::binding;

using ::takatori::util::fail;
using ::takatori::util::downcast;
using ::takatori::util::string_builder;
using namespace ::yugawara;
using namespace ::yugawara::variable;

using kind = meta::field_type_kind;
constexpr std::size_t max_char_len = 100;

class cli {
public:
    int operator()(params& param, std::shared_ptr<configuration> cfg) {
        map_thread_to_storage_ = init_map(param);
        auto num_threads = param.partitions_;
        std::vector<std::shared_ptr<plan::compiler_context>> contexts{num_threads};
        utils::get_watch().set_point(time_point_begin, 0);
        std::shared_ptr<kvs::database> db = kvs::database::open();
        threading_prepare_storage(param, db.get(), cfg.get(), contexts);
        utils::get_watch().set_point(time_point_storage_prepared, 0);
        threading_create_and_schedule_request(param, db, cfg, contexts);
        dump_perf_info();
        return 0;
    }

    void threading_prepare_storage(
        params& param,
        kvs::database* db,
        configuration const* cfg,
        std::vector<std::shared_ptr<plan::compiler_context>>& contexts
    ) {
        auto num_threads = param.partitions_;
        boost::thread_group thread_group{};
        std::vector<int> result(num_threads);
        for(std::size_t thread_id = 1; thread_id <= num_threads; ++thread_id) {
            auto thread = new boost::thread([&db, &cfg, thread_id, &param, this, &contexts]() {
                LOG(INFO) << "thread " << thread_id << " storage creation start";
                contexts[thread_id-1] = prepare_storage(param, db, thread_id, cfg);
                LOG(INFO) << "thread " << thread_id << " storage creation end";
            });
            set_core_affinity(thread, thread_id, cfg);
            thread_group.add_thread(thread);
        }
        thread_group.join_all();
    }

    void set_core_affinity(
        boost::thread* t,
        std::size_t thread_id,
        configuration const* cfg
    ) {
        if(cfg->core_affinity()) {
            auto cpu = thread_id+cfg->initial_core();
            if (cfg->assign_numa_nodes_uniformly()) {
                static std::size_t nodes = numa_max_node()+1;
                numa_run_on_node(static_cast<int>(cpu % nodes));
            } else {
                pthread_t x = t->native_handle();
                cpu_set_t cpuset;
                CPU_ZERO(&cpuset);
                CPU_SET(cpu, &cpuset);
                ::pthread_setaffinity_np(x, sizeof(cpu_set_t), &cpuset);
            }
        }
    }

    std::shared_ptr<plan::compiler_context> prepare_storage(params& param, kvs::database* db, std::size_t storage_id, configuration const* cfg) {
        (void)cfg;
        std::string table_name("T");
        table_name.append(std::to_string(storage_id));
        std::string index_name("I");
        index_name.append(std::to_string(storage_id));
        // generate takatori compile info and statement
        auto compiler_context = std::make_shared<plan::compiler_context>();
        create_compiled_info(compiler_context, table_name, index_name);

        compiler_context->storage_provider()->each_index([&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const&) {
            db->create_storage(id);
        });
        load_data(db, index_name, param);
        return compiler_context;
    }

    void threading_create_and_schedule_request(
        params& param,
        std::shared_ptr<kvs::database> db,
        std::shared_ptr<configuration> cfg,
        std::vector<std::shared_ptr<plan::compiler_context>>& contexts
        ) {
        auto num_threads = param.partitions_;
        boost::thread_group thread_group{};
        boost::latch prepare_completion_latch(num_threads);
        std::vector<int> result(num_threads);
        for(std::size_t thread_id = 1; thread_id <= num_threads; ++thread_id) {
            auto thread = new boost::thread([&db, &cfg, thread_id, &param, this, &contexts, &prepare_completion_latch]() {
                LOG(INFO) << "thread " << thread_id << " create request start";
                auto storage_id = map_thread_to_storage_[thread_id-1]+1;
                create_and_schedule_request(param, cfg, db, prepare_completion_latch, storage_id, contexts[storage_id-1]);
                LOG(INFO) << "thread " << thread_id << " schedule request end";
            });
            set_core_affinity(thread, thread_id, cfg.get());
            thread_group.add_thread(thread);
        }
        thread_group.join_all();
    }

    void create_and_schedule_request(
        params const& param,
        std::shared_ptr<configuration> cfg,
        std::shared_ptr<kvs::database> db,
        boost::latch& prepare_completion_latch,
        std::size_t thread_id,
        std::shared_ptr<plan::compiler_context> compiler_context
    ) {
        (void)prepare_completion_latch;
        // create step graph with only process
        auto& p = static_cast<takatori::statement::execute&>(compiler_context->statement()).execution_plan();
        auto& p0 = find_process(p);
        auto channel = std::make_shared<class channel>();
        memory::monotonic_paged_memory_resource record_resource{&global::page_pool()};
        memory::monotonic_paged_memory_resource varlen_resource{&global::page_pool()};
        request_context::result_stores stores{};
        auto context = std::make_shared<request_context>(
            channel,
            cfg,
            compiler_context,
            std::move(db),
            &stores,
            &record_resource,
            &varlen_resource
        );
        common::graph g{*context};
        g.emplace<process::step>(jogasaki::plan::impl::create(p0, *compiler_context));

        dag_controller dc{std::move(cfg)};
        utils::get_watch().set_point(time_point_request_created, thread_id);
        prepare_completion_latch.count_down_and_wait();
        utils::get_watch().set_point(time_point_schedule, thread_id);
        dc.schedule(g);
        utils::get_watch().set_point(time_point_completed, thread_id);
        dump_debug_data(stores, param);
    }

    void dump_debug_data(request_context::result_stores stores, params const& param) {
        if(param.debug) {
            auto store = stores[0];
            auto record_meta = store->meta();
            auto it = store->begin();
            while(it != store->end()) {
                auto record = it.ref();
                LOG(INFO) <<
                    "C0: " << record.get_value<std::int32_t>(record_meta->value_offset(0)) <<
                    " C1: " << record.get_value<std::int64_t>(record_meta->value_offset(1)) <<
                    " C2: " << record.get_value<double>(record_meta->value_offset(2)) <<
                    " C3: " << record.get_value<float>(record_meta->value_offset(3)) <<
                    " C4: " << record.get_value<accessor::text>(record_meta->value_offset(4));
                ++it;
            }

        }
    }
    void load_data(kvs::database* db, std::string_view storage_name, params& param) {
        auto tx = db->create_transaction();
        auto stg = db->get_storage(storage_name);

        std::string key_buf(1024, '\0');
        std::string val_buf(1024, '\0');
        kvs::stream key_stream{key_buf};
        kvs::stream val_stream{val_buf};

        using key_record = jogasaki::mock::basic_record<kind::int4, kind::int8>;
        using value_record = jogasaki::mock::basic_record<kind::float8, kind::float4, kind::character>;
        auto key_meta = key_record{}.record_meta();
        auto val_meta = value_record{}.record_meta();

        utils::xorshift_random64 rnd{};
        for(std::size_t i=0; i < param.records_per_partition_; ++i) {
            key_record key_rec{key_meta,
                static_cast<std::int32_t>(param.sequential_data ? i : rnd()),
                static_cast<std::int64_t>(param.sequential_data ? i*2 : rnd()),
            };
            kvs::encode(key_rec.ref(), key_meta->value_offset(0), key_meta->at(0), key_stream);
            kvs::encode(key_rec.ref(), key_meta->value_offset(1), key_meta->at(1), key_stream);
            std::string str(rnd() % max_char_len, static_cast<char>(param.sequential_data ? 'A'+(i % 26) : rnd()));
            value_record val_rec{val_meta,
                static_cast<double>(param.sequential_data ? i*10 : rnd()),
                static_cast<float>(param.sequential_data ? i*100 : rnd()),
                accessor::text(str.data(), str.size())
            };
            kvs::encode(val_rec.ref(), val_meta->value_offset(0), val_meta->at(0), val_stream);
            kvs::encode(val_rec.ref(), val_meta->value_offset(1), val_meta->at(1), val_stream);
            kvs::encode(val_rec.ref(), val_meta->value_offset(2), val_meta->at(2), val_stream);
            if(auto res = stg->put(*tx,
                std::string_view{key_buf.data(), key_stream.length()},
                std::string_view{val_buf.data(), val_stream.length()}
            ); !res) {
                fail();
            }
            key_stream.reset();
            val_stream.reset();
        }
        if (auto res = tx->commit(); !res) {
            fail();
        }
    }

    std::vector<std::size_t> init_map(params& param) {
        std::vector<std::size_t> ret{};
        ret.reserve(param.partitions_);
        for(std::size_t i=0; i < param.partitions_; ++i) {
            ret.emplace_back(i);
        }
        std::mt19937_64 mt{};
        std::shuffle(ret.begin(), ret.end(), mt);
        return ret;
    }
private:
    std::vector<std::size_t> map_thread_to_storage_{};
    memory::page_pool pool_;

    void dump_perf_info() {
        auto& watch = utils::get_watch();
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_begin, time_point_storage_prepared, "prepare storage");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_storage_prepared, time_point_request_created, "create request");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_request_created, time_point_schedule, "wait all requests");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_schedule, time_point_completed, "process request");
    }

    void create_compiled_info(
        std::shared_ptr<plan::compiler_context> compiler_context,
        std::string_view table_name,
        std::string_view index_name
    ) {
        binding::factory bindings;
        std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();

        std::shared_ptr<::yugawara::storage::table> t0 = storages->add_table(table_name, {
            table_name,
            {
                { "C0", t::int4() },
                { "C1", t::int8() },
                { "C2", t::float8() },
                { "C3", t::float4() },
                { "C4", t::character(t::varying, max_char_len) },
            },
        });
        std::shared_ptr<::yugawara::storage::index> i0 = storages->add_index(index_name, {
            t0,
            index_name,
            {
                t0->columns()[0],
                t0->columns()[1],
            },
            {},
            {
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
            },
        });

        storage::column const& t0c0 = t0->columns()[0];
        storage::column const& t0c1 = t0->columns()[1];
        storage::column const& t0c2 = t0->columns()[2];
        storage::column const& t0c3 = t0->columns()[3];
        storage::column const& t0c4 = t0->columns()[4];

        auto p = std::make_shared<takatori::plan::graph_type>();
        auto&& p0 = p->insert(takatori::plan::process {});
        auto c0 = bindings.stream_variable("c0");
        auto c1 = bindings.stream_variable("c1");
        auto c2 = bindings.stream_variable("c2");
        auto c3 = bindings.stream_variable("c3");
        auto c4 = bindings.stream_variable("c4");
        auto& r0 = p0.operators().insert(relation::scan {
            bindings(*i0),
            {
                { bindings(t0c0), c0 },
                { bindings(t0c1), c1 },
                { bindings(t0c2), c2 },
                { bindings(t0c3), c3 },
                { bindings(t0c4), c4 },
            },
        });

        auto&& r1 = p0.operators().insert(relation::emit {
            {
                { c0, "c0"},
                { c1, "c1"},
                { c2, "c2"},
                { c3, "c3"},
                { c4, "c4"},
            },
        });

        r0.output() >> r1.input();

        auto vm = std::make_shared<yugawara::analyzer::variable_mapping>();
        vm->bind(c0, t::int4{});
        vm->bind(c1, t::int8{});
        vm->bind(c2, t::float8{});
        vm->bind(c3, t::float4{});
        vm->bind(c4, t::character{t::varying, max_char_len});
        vm->bind(bindings(t0c0), t::int4{});
        vm->bind(bindings(t0c1), t::int8{});
        vm->bind(bindings(t0c2), t::float8{});
        vm->bind(bindings(t0c3), t::float4{});
        vm->bind(bindings(t0c4), t::character{t::varying, max_char_len});
        yugawara::compiled_info c_info{{}, vm};

        compiler_context->storage_provider(std::move(storages));
        compiler_context->compiled_info(c_info);
        compiler_context->statement(std::make_unique<takatori::statement::execute>(std::move(*p)));
    }

    takatori::plan::process& find_process(takatori::plan::graph_type& p) {
        takatori::plan::process* p0{};
        takatori::plan::sort_from_upstream(p, [&p0](takatori::plan::step& s){
            if (s.kind() == takatori::plan::step_kind::process) {
                p0 = &dynamic_cast<takatori::plan::process&>(s);
            }
        });
        if (! p0) fail();
        return *p0;
    }
}; // class cli

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("scan cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("scan cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1) {
        gflags::ShowUsageWithFlags(argv[0]); // NOLINT
        return -1;
    }

    jogasaki::scan_cli::params s{};
    auto cfg = std::make_shared<jogasaki::configuration>();
    cfg->single_thread(!FLAGS_use_multithread);
    cfg->thread_pool_size(FLAGS_thread_pool_size);

    s.partitions_ = FLAGS_partitions;
    s.records_per_partition_ = FLAGS_records_per_partition;
    s.debug = FLAGS_debug;
    s.sequential_data = FLAGS_sequential_data;

    cfg->core_affinity(FLAGS_core_affinity);
    cfg->initial_core(FLAGS_initial_core);
    cfg->assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);

    if (FLAGS_minimum) {
        cfg->single_thread(true);
        cfg->thread_pool_size(1);
        cfg->initial_core(1);
        cfg->core_affinity(false);

        s.partitions_ = 1;
        s.records_per_partition_ = 3;
    }

    if (cfg->assign_numa_nodes_uniformly()) {
        cfg->core_affinity(true);
    }

    try {
        jogasaki::scan_cli::cli{}(s, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
