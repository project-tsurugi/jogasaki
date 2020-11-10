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

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/runtime_feature.h>
#include <yugawara/compiler.h>
#include <yugawara/compiler_options.h>
#include <yugawara/binding/factory.h>

#include <takatori/type/int.h>
#include <takatori/type/float.h>
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

using custom_memory_resource = jogasaki::memory::monotonic_paged_memory_resource;

using kind = meta::field_type_kind;
using test_record = jogasaki::mock::basic_record<kind::float8, kind::int4, kind::int8>;
using reader_type = process::mock::basic_record_reader<test_record>;
using writer_type = process::mock::basic_record_writer<test_record>;

class cli {
public:
    int operator()(params& param, std::shared_ptr<configuration> cfg) {
        utils::get_watch().set_point(time_point_begin, 0);
        assert(test_record{}.record_meta()->record_size() == 24); //NOLINT
        static_assert(sizeof(test_record) == 48); // record_ref + maybe_shared_ptr
        auto meta = test_record{}.record_meta();

        // generate takatori compile info and statement
        auto compiler_context = std::make_shared<plan::compiler_context>();
        create_compiled_info(compiler_context);

        auto db = kvs::database::open();
        compiler_context->storage_provider()->each_index([&](std::string_view id, std::shared_ptr<yugawara::storage::index const> const&) {
            db->create_storage(id);
        });
        load_data(db.get(), "I0", param);

        // create step graph with only process
        auto& p = static_cast<takatori::statement::execute&>(compiler_context->statement()).execution_plan();
        auto& p0 = find_process(p);
        auto channel = std::make_shared<class channel>();
        auto context = std::make_shared<request_context>(channel, cfg, compiler_context, std::shared_ptr<kvs::database>(std::move(db)));
        common::graph g{*context};
        g.emplace<process::step>(jogasaki::plan::impl::create(p0, *compiler_context));

        dag_controller dc{std::move(cfg)};
        utils::get_watch().set_point(time_point_schedule, 0);
        dc.schedule(g);
        utils::get_watch().set_point(time_point_completed, 0);
        dump_perf_info();

        return 0;
    }

    void load_data(kvs::database* db, std::string_view storage_name, params& param) {
        auto tx = db->create_transaction();
        auto stg = db->get_storage(storage_name);

        std::string key_buf(100, '\0');
        std::string val_buf(100, '\0');
        kvs::stream key_stream{key_buf};
        kvs::stream val_stream{val_buf};

        using key_record = jogasaki::mock::basic_record<kind::int4>;
        using value_record = jogasaki::mock::basic_record<kind::float8, kind::int8>;
        auto key_meta = key_record{}.record_meta();
        auto val_meta = value_record{}.record_meta();

        utils::xorshift_random64 rnd{};
        for(std::size_t i=0; i < param.records_per_partition_; ++i) {
            key_record key_rec{key_meta, static_cast<std::int32_t>(rnd())};
            kvs::encode(key_rec.ref(), key_meta->value_offset(0), key_meta->at(0), key_stream);
            value_record val_rec{val_meta, static_cast<double>(rnd()), static_cast<std::int64_t>(rnd())};
            kvs::encode(val_rec.ref(), val_meta->value_offset(0), val_meta->at(0), val_stream);
            kvs::encode(val_rec.ref(), val_meta->value_offset(1), val_meta->at(1), val_stream);
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
private:
    memory::page_pool pool_;
    std::vector<std::shared_ptr<custom_memory_resource>> resources_{};
    std::vector<maybe_shared_ptr<takatori::plan::exchange>> input_exchanges_{};
    std::vector<maybe_shared_ptr<takatori::plan::exchange>> output_exchanges_{};
    std::vector<std::shared_ptr<writer_type>> writers_{};
    std::vector<std::shared_ptr<reader_type>> readers_{};

    void dump_perf_info() {
        auto& watch = utils::get_watch();
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_begin, time_point_schedule, "create graph");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_schedule, time_point_create_task, "schedule");
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_create_task, time_point_created_task, "create tasks");
#ifndef PERFORMANCE_TOOLS
        LOG(INFO) << "wait before run: total " << watch.duration(time_point_created_task, time_point_run) << "ms" ;
#endif
        LOG(INFO) << jogasaki::utils::textualize(watch, time_point_run, time_point_ran, "run");
#ifndef PERFORMANCE_TOOLS
        LOG(INFO) << "finish: total " << watch.duration(time_point_ran, time_point_completed) << "ms" ;
#endif
    }

    void create_compiled_info(std::shared_ptr<plan::compiler_context> compiler_context) {
        binding::factory bindings;
        std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();

        std::shared_ptr<::yugawara::storage::table> t0 = storages->add_table("T0", {
            "T0",
            {
                { "C0", t::int4() },
                { "C1", t::float8() },
                { "C2", t::int8() },
            },
        });
        std::shared_ptr<::yugawara::storage::index> i0 = storages->add_index("I0", {
            t0,
            "I0",
            {
                t0->columns()[0],
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

        auto p = std::make_shared<takatori::plan::graph_type>();
        auto&& p0 = p->insert(takatori::plan::process {});
        auto c0 = bindings.stream_variable("c0");
        auto c1 = bindings.stream_variable("c1");
        auto c2 = bindings.stream_variable("c2");
        auto& r0 = p0.operators().insert(relation::scan {
            bindings(*i0),
            {
                { bindings(t0c0), c0 },
                { bindings(t0c1), c1 },
                { bindings(t0c2), c2 },
            },
        });

        auto&& r1 = p0.operators().insert(relation::emit {
            {
                { c0, "c0"},
                { c1, "c1"},
                { c2, "c2"},
            },
        });

        r0.output() >> r1.input();

        auto vm = std::make_shared<yugawara::analyzer::variable_mapping>();
        vm->bind(c0, t::int4{});
        vm->bind(c1, t::float8{});
        vm->bind(c2, t::int8{});
        vm->bind(bindings(t0c0), t::int4{});
        vm->bind(bindings(t0c1), t::float8{});
        vm->bind(bindings(t0c2), t::int8{});
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

    /*
    void customize_process(
        params& param,
        process::step& process,
        maybe_shared_ptr<meta::record_meta> meta
    ) {
        // create custom contexts
        utils::xorshift_random64 rnd{1234567U};
        auto records_per_partition = param.records_per_partition_;
        auto record_size = sizeof(test_record);
        auto write_buffer_record_count = param.write_buffer_size_ / record_size;
        auto read_buffer_record_count = param.read_buffer_size_ / record_size;
        auto partitions = param.partitions_;
        std::vector<std::shared_ptr<process::abstract::task_context>> custom_contexts{};
        if (param.std_allocator) {
            resources_.reserve(partitions*2);
        }
        for(std::size_t i=0; i < partitions; ++i) {
            pmr::memory_resource* reader_resource =
                param.std_allocator ?
                    static_cast<pmr::memory_resource*>(takatori::util::get_standard_memory_resource()) :
                    resources_.emplace_back(std::make_shared<custom_memory_resource>(&pool_)).get();
            std::size_t seq = 0;
            auto& reader = readers_.emplace_back(std::make_shared<reader_type>(
                read_buffer_record_count,
                (records_per_partition + read_buffer_record_count - 1)/ read_buffer_record_count,
                [&rnd, &meta, &seq, &param]() {
                    ++seq;
                    return test_record{
                        meta,
                        static_cast<double>(param.sequential_data ? seq : rnd()),
                        static_cast<std::int32_t>(param.sequential_data ? seq*10 : rnd()),
                        static_cast<std::int64_t>(param.sequential_data ? seq*100 : rnd()),
                    };
                },
                reader_resource
                )
            );
            reader_container r{reader.get()};
            pmr::memory_resource* writer_resource =
                param.std_allocator ?
                    static_cast<pmr::memory_resource*>(takatori::util::get_standard_memory_resource()) :
                    resources_.emplace_back(std::make_shared<custom_memory_resource>(&pool_)).get();
            auto& writer = writers_.emplace_back(std::make_shared<writer_type>(
                write_buffer_record_count,
                writer_resource
            ));
            auto ctx =
                std::make_shared<process::mock::task_context>(
                    std::vector<reader_container>{r},
                    std::vector<std::shared_ptr<executor::record_writer>>{writer},
                    std::vector<std::shared_ptr<executor::record_writer>>{},
                    std::shared_ptr<abstract::scan_info>{}
                );

            ctx->work_context(std::make_unique<process::impl::work_context>(
                2UL, // operator count
                1UL, // block_scope count
                std::make_unique<memory::lifo_paged_memory_resource>(&pool_),
                std::shared_ptr<kvs::database>{}
            ));
            custom_contexts.emplace_back(std::move(ctx));
        }

        // insert custom contexts via executor factory
        auto f = std::make_shared<abstract::process_executor_factory>([custom_contexts = std::move(custom_contexts)](
            std::shared_ptr<abstract::processor> processor,
            std::vector<std::shared_ptr<abstract::task_context>> contexts //NOLINT
        ){
            (void)contexts;
            auto ret = std::make_shared<process::mock::process_executor>(std::move(processor), custom_contexts);

            ret->will_run(
                std::make_shared<callback_type>([](callback_arg* arg){
                    utils::get_watch().set_point(time_point_run, arg->identity_);
                })
            );
            ret->did_run(
                std::make_shared<callback_type>([](callback_arg* arg){
                    utils::get_watch().set_point(time_point_ran, arg->identity_);
                })
            );
            return ret;
        });
        process.executor_factory(f);
        process.partitions(partitions);
        process.will_create_tasks(
            std::make_shared<callback_type>([](callback_arg*){
                utils::get_watch().set_point(time_point_create_task, 0);
            })
        );
        process.did_create_tasks(
            std::make_shared<callback_type>([](callback_arg*){
                utils::get_watch().set_point(time_point_created_task, 0);
            })
        );

    }
     */
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

    cfg->core_affinity(FLAGS_core_affinity);
    cfg->initial_core(FLAGS_initial_core);
    cfg->assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);

    if (FLAGS_minimum) {
        cfg->single_thread(true);
        cfg->thread_pool_size(1);
        cfg->initial_core(1);
        cfg->core_affinity(false);

        s.partitions_ = 1;
        s.records_per_partition_ = 1;
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
