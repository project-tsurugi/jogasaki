/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <boost/move/utility_core.hpp>
#include <gflags/gflags.h>
#include <glog/logging.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/exchange/forward/step.h>
#include <jogasaki/executor/exchange/mock/aggregate/shuffle_info.h>
#include <jogasaki/executor/exchange/mock/aggregate/step.h>
#include <jogasaki/executor/exchange/step.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/model/port.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/latch_set.h>

#include "../common/aggregator.h"
#include "../common/producer_constants.h"
#include "../common/show_perf_info.h"
#include "consumer_process.h"
#include "params.h"
#include "producer_process.h"

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
DEFINE_string(proffile, "", "Performance measurement result file.");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_int64(key_modulo, -1, "key value integer is calculated based on the given modulo. Specify -1 to disable.");  //NOLINT
DEFINE_int32(prepare_pages, 600, "prepare specified number of memory pages per partition that are first touched beforehand. Specify -1 to disable.");  //NOLINT

namespace jogasaki::mock_aggregate_cli {

using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::executor::exchange::mock::aggregate;
using namespace jogasaki::scheduler;

std::shared_ptr<meta::record_meta> test_record_meta() {
    return std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                    meta::field_type(meta::field_enum_tag<meta::field_type_kind::int8>),
                    meta::field_type(meta::field_enum_tag<meta::field_type_kind::float8>),
            },
            boost::dynamic_bitset<std::uint64_t>{std::string("00")});
}

using key_type = std::int64_t;
using value_type = double;

static int run(params& s, std::shared_ptr<configuration> cfg) {
    auto meta = test_record_meta();
    auto aggregator = common_cli::create_aggregator();
    auto info = std::make_shared<shuffle_info>(meta, std::vector<std::size_t>{0}, std::move(aggregator));

    auto compiler_context = std::make_shared<plan::compiler_context>();
    auto context = std::make_shared<request_context>(cfg);
    prepare_scheduler(*context);

    global::config_pool(cfg);
    common::graph g{};
    auto& scan = g.emplace<producer_process>(meta, s);
    auto& xch = g.emplace<mock::aggregate::step>(info, meta::variable_order{}, meta::variable_order{});
    auto& emit = g.emplace<consumer_process>(info->group_meta(), s);
    auto& fwd = g.emplace<forward::step>();
    scan >> xch;
    xch >> emit;
    emit >> fwd;

    jogasaki::utils::get_latches().enable(sync_wait_prepare,
        std::min(s.upstream_partitions_, cfg->thread_pool_size()));
    dag_controller dc{std::move(cfg)};
    dc.schedule(g, *context);
    return 0;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("mock aggregate cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("mock aggregate cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    jogasaki::mock_aggregate_cli::params s{};
    auto cfg = std::make_shared<jogasaki::configuration>();
    cfg->single_thread(!FLAGS_use_multithread);
    cfg->thread_pool_size(FLAGS_thread_pool_size);

    s.upstream_partitions_ = FLAGS_upstream_partitions;
    s.downstream_partitions_ = FLAGS_downstream_partitions;
    s.records_per_upstream_partition_ = FLAGS_records_per_partition;
    s.key_modulo_ = FLAGS_key_modulo;
    s.prepare_pages_ = FLAGS_prepare_pages;

    cfg->core_affinity(FLAGS_core_affinity);
    cfg->initial_core(FLAGS_initial_core);
    cfg->assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);


    if (FLAGS_minimum) {
        cfg->single_thread(true);
        cfg->thread_pool_size(1);
        cfg->initial_core(1);
        cfg->core_affinity(false);

        s.upstream_partitions_ = 1;
        s.downstream_partitions_ = 1;
        s.records_per_upstream_partition_ = 1;
    }

    if (cfg->assign_numa_nodes_uniformly()) {
        cfg->core_affinity(true);
    }

    if (cfg->thread_pool_size() < s.upstream_partitions_) {
        LOG(WARNING) << "thread pool size (" << cfg->thread_pool_size() << ") is smaller than the number of upstream partitions(" << s.upstream_partitions_ << ") Not all of them are processed concurrently.";
    }
    if (cfg->thread_pool_size() < s.downstream_partitions_) {
        LOG(WARNING) << "thread pool size (" << cfg->thread_pool_size() << ") is smaller than the number of downstream partitions(" << s.downstream_partitions_ << ") Not all of them are processed concurrently.";
    }
    try {
        jogasaki::mock_aggregate_cli::run(s, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    jogasaki::common_cli::show_perf_info();

    return 0;
}
