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
#include <unordered_map>

#include <glog/logging.h>

#include <takatori/graph/graph.h>
#include <takatori/plan/forward.h>
#include <takatori/plan/process.h>
#include <takatori/relation/expression.h>
#include <takatori/type/int.h>
#include <takatori/type/float.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/exchange/deliver/step.h>
#include <jogasaki/executor/exchange/group/shuffle_info.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/plan/relation_step_map.h>
#include <jogasaki/constants.h>
#include <jogasaki/utils/performance_tools.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/executor/process/impl/ops/offer.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/executor/process/mock/record_writer.h>
#include <jogasaki/executor/process/mock/record_reader.h>
#include <jogasaki/executor/process/mock/process_executor.h>
#include <jogasaki/executor/process/impl/process_executor.h>
#include <jogasaki/executor/process/impl/work_context.h>

#include "params.h"
#include <jogasaki/executor/process/mock/task_context.h>
#include <takatori/statement/execute.h>
#include <jogasaki/executor/process/impl/ops/take_flat.h>

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

std::shared_ptr<meta::record_meta> test_record_meta() {
    return std::make_shared<meta::record_meta>(
        std::vector<meta::field_type>{
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::float8>),
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int4>),
            meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int8>),
        },
        boost::dynamic_bitset<std::uint64_t>{std::string("000")});
}

static int run(params& param, std::shared_ptr<configuration> cfg) {
    auto meta = test_record_meta();

    (void)param;

    binding::factory bindings;
    std::shared_ptr<storage::configurable_provider> storages = std::make_shared<storage::configurable_provider>();
    std::shared_ptr<storage::table> t0 = storages->add_table("T0", {
        "T0",
        {
            { "C0", t::int4() },
            { "C1", t::float8() },
            { "C2", t::int8() },
        },
    });
    storage::column const& t0c0 = t0->columns()[0];
    storage::column const& t0c1 = t0->columns()[1];
    storage::column const& t0c2 = t0->columns()[2];

    std::shared_ptr<storage::index> i0 = storages->add_index("I0", { t0, "I0", });

    ::takatori::plan::forward f0 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f0c0 = f0.columns()[0];
    auto&& f0c1 = f0.columns()[1];
    auto&& f0c2 = f0.columns()[2];

    ::takatori::plan::forward f1 {
        bindings.exchange_column(),
        bindings.exchange_column(),
        bindings.exchange_column(),
    };
    auto&& f1c0 = f1.columns()[0];
    auto&& f1c1 = f1.columns()[1];
    auto&& f1c2 = f1.columns()[2];

    takatori::plan::graph_type p;
    auto&& p0 = p.insert(takatori::plan::process {});
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

    auto p_info = std::make_shared<processor_info>(p0.operators(), c_info);
    auto compiler_context = std::make_shared<plan::compiler_context>();
    compiler_context->compiled_info(c_info);
    compiler_context->statement(std::make_unique<takatori::statement::execute>(std::move(p)));

    std::vector<variable> f0_columns{f0c1, f0c0, f0c2};
    variable_order f0_order{
        variable_ordering_enum_tag<variable_ordering_kind::flat_record>,
        f0_columns
    };
    std::vector<variable> f1_columns{f1c1, f1c0, f1c2};
    variable_order f1_order{
        variable_ordering_enum_tag<variable_ordering_kind::flat_record>,
        f1_columns
    };

    auto channel = std::make_shared<class channel>();
    auto context = std::make_shared<request_context>(channel, cfg, compiler_context);
    common::graph g{*context};
    auto& jf0 = g.emplace<forward::step>(
        meta,
        f0_order
    );
    auto& jf1 = g.emplace<forward::step>(
        meta,
        f1_order
    );

    auto r_step_map = std::make_shared<plan::relation_step_map>(
        std::unordered_map<takatori::descriptor::relation, executor::exchange::step*>{
        {bindings(f0), &jf0},
        {bindings(f1), &jf1},
    });
    compiler_context->relation_step_map(r_step_map);

    std::vector<take_flat::column> take_flat_columns{
        {f0c0, c0},
        {f0c1, c1},
        {f0c2, c2},
    };
    take_flat t{
        *p_info,
        0,
        f0_order,
        meta,
        take_flat_columns,
        0,
        &r1
    };

    std::vector<offer::column> offer_columns {
        {c0, f1c0},
        {c1, f1c1},
        {c2, f1c2},
    };

    offer s{
        *p_info,
        0,
        f1_order,
        meta,
        offer_columns,
        0
    };

    auto& scope_info = p_info->scopes_info()[s.block_index()];
    block_scope variables{scope_info};

    using kind = meta::field_type_kind;
    using test_record = jogasaki::mock::basic_record<kind::float8, kind::int4, kind::int8>;
//    executor::process::mock::task_context task_ctx{
//        {reader_container{reader.get()}},
//        {writer},
//        {},
//        {},
//    };

    auto& process = g.emplace<process::step>(p_info);
    jf0 >> process >> jf1;


    using reader_type = process::mock::basic_record_reader<test_record>;
    using writer_type = process::mock::basic_record_writer<test_record>;

    auto partitions = param.partitions_;
    auto records_per_partition = param.records_per_partition_;
    std::vector<std::shared_ptr<process::abstract::task_context>> custom_contexts{};
    std::vector<std::shared_ptr<writer_type>> writers{};
    std::vector<std::shared_ptr<reader_type>> readers{};
    for(std::size_t i=0; i < partitions; ++i) {
        std::vector<test_record> records{
            test_record{1.0, 10, 100},
            test_record{2.0, 20, 200},
            test_record{3.0, 30, 300},
            test_record{4.0, 40, 400},
            test_record{5.0, 50, 500},
        };
        auto& reader = readers.emplace_back(std::make_shared<reader_type>(records));
        reader->repeats(records_per_partition / 5);
        reader_container r{reader.get()};
        auto& writer = writers.emplace_back(std::make_shared<writer_type>());
        auto ctx =
            std::make_shared<process::mock::task_context>(
                std::vector<reader_container>{r},
                std::vector<std::shared_ptr<executor::record_writer>>{writer},
                std::vector<std::shared_ptr<executor::record_writer>>{},
                std::shared_ptr<abstract::scan_info>{}
            );

        ctx->work_context(std::make_unique<process::impl::work_context>());
        custom_contexts.emplace_back(std::move(ctx));
    }
    auto f = std::make_shared<abstract::process_executor_factory>([&custom_contexts](
        std::shared_ptr<abstract::processor> processor,
        std::vector<std::shared_ptr<abstract::task_context>> contexts
        ){
        (void)contexts;
        return std::make_shared<process::mock::process_executor>(std::move(processor), std::move(custom_contexts));
    });
    process.executor_factory(f);
    process.partitions(partitions);

    dag_controller dc{std::move(cfg)};
    dc.schedule(g);

    for(auto&& w : writers) {
        LOG(INFO) << "written " << w->size() << " records";
    }
    return 0;
}

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
        jogasaki::process_cli::run(s, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
