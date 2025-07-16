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
#include <cstddef>
#include <exception>
#include <iostream>
#include <memory>
#include <cstdint>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <boost/container/container_fwd.hpp>
#include <boost/container/pmr/global_resource.hpp>
#include <boost/container/pmr/memory_resource.hpp>
#include <boost/move/utility_core.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <takatori/graph/graph.h>
#include <takatori/graph/port.h>
#include <takatori/plan/exchange.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/graph.h>
#include <takatori/plan/process.h>
#include <takatori/plan/step.h>
#include <takatori/plan/step_kind.h>
#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/take_flat.h>
#include <takatori/statement/execute.h>
#include <takatori/type/primitive.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/meta_type.h>
#include <yugawara/analyzer/variable_mapping.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/callback.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/exchange/forward/source.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/abstract/process_executor.h>
#include <jogasaki/executor/process/abstract/processor.h>
#include <jogasaki/executor/process/abstract/range.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/impl/ops/default_value_kind.h>
#include <jogasaki/executor/process/impl/ops/io_info.h>
#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/impl/work_context.h>
#include <jogasaki/executor/process/mock/process_executor.h>
#include <jogasaki/executor/process/mock/record_reader.h>
#include <jogasaki/executor/process/mock/record_writer.h>
#include <jogasaki/executor/process/mock/task_context.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/monotonic_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/names.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/plan/executable_statement.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/performance_tools.h>
#include <jogasaki/utils/random.h>
#include <jogasaki/utils/watch.h>

#include "cli_constants.h"
#include "params.h"

#ifdef ENABLE_GOOGLE_PERFTOOLS
#include "gperftools/profiler.h"
#endif

DEFINE_int32(thread_pool_size, 3, "Thread pool size");  //NOLINT
DEFINE_bool(use_multithread, true, "whether using multiple threads");  //NOLINT
DEFINE_int32(partitions, 3, "Number of partitions");  //NOLINT
DEFINE_int32(records_per_partition, 100000, "Number of records per partition");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_string(proffile, "", "Performance measurement result file.");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_int32(write_buffer_size, 2097152, "Writer buffer size in byte");  //NOLINT
DEFINE_int32(read_buffer_size, 2097152, "Reader buffer size in byte");  //NOLINT
DEFINE_bool(std_allocator, false, "use standard allocator for reader/writer");  //NOLINT
DEFINE_bool(sequential_data, false, "use sequential data instead of randomly generated");  //NOLINT
DEFINE_int32(randomize_memory, 0, "initialize each thread with randomly allocated memory. Specify magnitude for randomizer. Specify 0 to disable.");  //NOLINT

namespace jogasaki::process_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;

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

using namespace boost::container;

using custom_memory_resource = jogasaki::memory::monotonic_paged_memory_resource;

using kind = meta::field_type_kind;
using test_record = jogasaki::mock::basic_record;
using reader_type = process::mock::basic_record_reader;
using writer_type = process::mock::basic_record_writer;

class cli {
public:
    int operator()(params& param, std::shared_ptr<configuration> cfg) {
        utils::get_watch().set_point(time_point_begin, 0);
        auto meta = jogasaki::mock::create_meta<kind::float8, kind::int4, kind::int8>(true);

        // generate takatori compile info and statement
        auto compiler_context = std::make_shared<plan::compiler_context>();
        create_compiled_info(compiler_context);

        // create step graph with only process
        auto& p = unsafe_downcast<takatori::statement::execute>(*compiler_context->executable_statement()->statement()).execution_plan();
        auto& p0 = find_process(p);
        auto context = std::make_shared<request_context>(cfg);
        prepare_scheduler(*context);
        global::config_pool(cfg);
        common::graph g{};

        auto& info = compiler_context->executable_statement()->compiled_info();
        auto& mirrors = compiler_context->executable_statement()->mirrors();
        jogasaki::plan::impl::preprocess(p0, info, mirrors);
        auto& process = g.emplace<process::step>(jogasaki::plan::impl::create(p0, info, mirrors, nullptr));
        customize_process(
            param,
            process,
            meta,
            context.get()
        );

        dag_controller dc{std::move(cfg)};
        utils::get_watch().set_point(time_point_schedule, 0);
        dc.schedule(g, *context);
        utils::get_watch().set_point(time_point_completed, 0);
        dump_perf_info();

        return 0;
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

    void create_compiled_info(std::shared_ptr<plan::compiler_context> const& compiler_context) {
        binding::factory bindings;
        std::shared_ptr<yugawara::storage::configurable_provider> storages = std::make_shared<yugawara::storage::configurable_provider>();
        std::shared_ptr<yugawara::storage::table> t0 = storages->add_table({
            "T0",
            {
                { "C0", t::int4() },
                { "C1", t::float8() },
                { "C2", t::int8() },
            },
        });
        yugawara::storage::column const& t0c0 = t0->columns()[0];
        yugawara::storage::column const& t0c1 = t0->columns()[1];
        yugawara::storage::column const& t0c2 = t0->columns()[2];

        std::shared_ptr<yugawara::storage::index> i0 = storages->add_index({ t0, "I0", });
        auto p = std::make_shared<takatori::plan::graph_type>();
        auto& f0 = p->insert(::takatori::plan::forward{
            variable_vector{
                bindings.exchange_column(),
                bindings.exchange_column(),
                bindings.exchange_column()
            }
        });
        auto&& f0c0 = f0.columns()[0];
        auto&& f0c1 = f0.columns()[1];
        auto&& f0c2 = f0.columns()[2];

        auto& f1 = p->insert(::takatori::plan::forward{
            variable_vector{
                bindings.exchange_column(),
                bindings.exchange_column(),
                bindings.exchange_column()
            }
        });
        auto&& f1c0 = f1.columns()[0];
        auto&& f1c1 = f1.columns()[1];
        auto&& f1c2 = f1.columns()[2];

        auto&& p0 = p->insert(takatori::plan::process {});
        auto c0 = bindings.stream_variable("c0");
        auto c1 = bindings.stream_variable("c1");
        auto c2 = bindings.stream_variable("c2");
        auto& r0 = p0.operators().insert(relation::step::take_flat {
            bindings.exchange(f0),
            {
                { f0c0, c0 },
                { f0c1, c1 },
                { f0c2, c2 },
            },
        });

        auto&& r1 = p0.operators().insert(relation::step::offer {
            bindings.exchange(f1),
            {
                { c0, f1c0 },
                { c1, f1c1 },
                { c2, f1c2 },
            },
        });

        r0.output() >> r1.input();

        f0.add_downstream(p0);
        f1.add_upstream(p0);
        auto vm = std::make_shared<yugawara::analyzer::variable_mapping>();
        vm->bind(c0, t::int4{});
        vm->bind(c1, t::float8{});
        vm->bind(c2, t::int8{});
        vm->bind(f0c0, t::int4{});
        vm->bind(f0c1, t::float8{});
        vm->bind(f0c2, t::int8{});
        vm->bind(f1c0, t::int4{});
        vm->bind(f1c1, t::float8{});
        vm->bind(f1c2, t::int8{});
        vm->bind(bindings(t0c0), t::int4{});
        vm->bind(bindings(t0c1), t::float8{});
        vm->bind(bindings(t0c2), t::int8{});
        yugawara::compiled_info c_info{{}, vm};
        auto mirrors = std::make_shared<plan::mirror_container>();
        jogasaki::plan::impl::preprocess(p0, c_info, mirrors);

        input_exchanges_.emplace_back(&f0);
        output_exchanges_.emplace_back(&f1);

        compiler_context->executable_statement(
            std::make_shared<plan::executable_statement>(
                std::make_shared<takatori::statement::execute>(std::move(*p)),
                c_info,
                std::shared_ptr<model::statement>{},
                std::shared_ptr<variable_table_info>{},
                std::shared_ptr<variable_table>{},
                std::move(mirrors),
                std::make_shared<std::string>("<sql text>")
            )
        );
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

    void customize_process(
        params& param,
        process::step& process,
        maybe_shared_ptr<meta::record_meta> meta,
        request_context* req_context
    ) {
        // create custom contexts
        utils::xorshift_random64 rnd{1234567U};
        auto records_per_partition = param.records_per_partition_;
        auto record_size = sizeof(test_record); // use object size because they are placed in read/write buffers
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
                    static_cast<pmr::memory_resource*>(pmr::get_default_resource()) :
                    resources_.emplace_back(std::make_shared<custom_memory_resource>(&pool_)).get();
            std::size_t seq = 0;
            auto& reader = readers_.emplace_back(std::make_shared<reader_type>(
                read_buffer_record_count,
                (records_per_partition + read_buffer_record_count - 1)/ read_buffer_record_count,
                [&rnd, &meta, &seq, &param]() {
                    ++seq;
                    return jogasaki::mock::create_record<kind::float8, kind::int4, kind::int8>(
                        meta,
                        static_cast<double>(param.sequential_data ? seq : rnd()),
                        static_cast<std::int32_t>(param.sequential_data ? seq*10 : rnd()),
                        static_cast<std::int64_t>(param.sequential_data ? seq*100 : rnd())
                    );
                },
                reader_resource
                )
            );
            io::reader_container r{reader.get()};
            pmr::memory_resource* writer_resource =
                param.std_allocator ?
                    static_cast<pmr::memory_resource*>(pmr::get_default_resource()) :
                    resources_.emplace_back(std::make_shared<custom_memory_resource>(&pool_)).get();
            auto& writer = writers_.emplace_back(jogasaki::executor::process::mock::create_writer_shared<kind::float8, kind::int4, kind::int8>(
                write_buffer_record_count,
                writer_resource
            ));
            auto ctx =
                std::make_shared<process::mock::task_context>(
                    std::vector<io::reader_container>{r},
                    std::vector<std::shared_ptr<executor::io::record_writer>>{writer},
                    std::shared_ptr<executor::io::record_writer>{},
                    std::shared_ptr<abstract::range>{}
                );

            ctx->work_context(std::make_unique<process::impl::work_context>(
                req_context,
                2UL, // operator count
                1UL, // variable_table count
                std::make_unique<memory::lifo_paged_memory_resource>(&pool_),
                std::make_unique<memory::lifo_paged_memory_resource>(&pool_),
                std::shared_ptr<kvs::database>{},
                std::shared_ptr<transaction_context>{},
                false,
                false
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
            std::make_shared<callback_type>([](callback_arg* /* unused */){
                utils::get_watch().set_point(time_point_create_task, 0);
            })
        );
        process.did_create_tasks(
            std::make_shared<callback_type>([](callback_arg* /* unused */){
                utils::get_watch().set_point(time_point_created_task, 0);
            })
        );

        auto& f0 = unsafe_downcast<takatori::plan::forward>(*input_exchanges_[0]);
        auto& f1 = unsafe_downcast<takatori::plan::forward>(*output_exchanges_[0]);
        process.io_info(
            std::make_shared<io_info>(
                std::vector<input_info>{
                    {
                        meta,
                        variable_order{
                            variable_ordering_enum_tag<variable_ordering_kind::flat_record>,
                            f0.columns()
                        }
                    }
                },
                std::vector<output_info>{
                    {
                        meta,
                        variable_order{
                            variable_ordering_enum_tag<variable_ordering_kind::flat_record>,
                            f1.columns()
                        }
                    }
                },
                io_info::external_output_entity_type{}
            )
        );
    }
}; // class cli

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("process cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("process cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1) {
        gflags::ShowUsageWithFlags(argv[0]); // NOLINT
        return -1;
    }

    jogasaki::process_cli::params s{};
    auto cfg = std::make_shared<jogasaki::configuration>();
    cfg->single_thread(!FLAGS_use_multithread);
    cfg->thread_pool_size(FLAGS_thread_pool_size);
    cfg->randomize_memory_usage(FLAGS_randomize_memory);

    s.partitions_ = FLAGS_partitions;
    s.records_per_partition_ = FLAGS_records_per_partition;
    s.read_buffer_size_ = FLAGS_read_buffer_size;
    s.write_buffer_size_ = FLAGS_write_buffer_size;
    s.std_allocator = FLAGS_std_allocator;
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
        s.records_per_partition_ = 1;
    }

    if (cfg->assign_numa_nodes_uniformly()) {
        cfg->core_affinity(true);
    }

    try {
        jogasaki::process_cli::cli{}(s, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
