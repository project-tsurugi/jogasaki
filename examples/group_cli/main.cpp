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

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/exchange/deliver/step.h>
#include <jogasaki/executor/exchange/group/group_info.h>
#include <jogasaki/constants.h>
#include <jogasaki/utils/performance_tools.h>

#include "producer_process.h"
#include "consumer_process.h"
#include "params.h"
#include "../common/cli_constants.h"
#include "../common/dump.h"

#ifdef ENABLE_GOOGLE_PERFTOOLS
#include "gperftools/profiler.h"
#endif

DEFINE_int32(thread_pool_size, 10, "Thread pool size");  //NOLINT
DEFINE_bool(use_multithread, true, "whether using multiple threads");  //NOLINT
DEFINE_int32(downstream_partitions, 10, "Number of downstream partitions");  //NOLINT
DEFINE_int32(upstream_partitions, 10, "Number of upstream partitions");  //NOLINT
DEFINE_int32(records_per_partition, 100000, "Number of records per partition");  //NOLINT
DEFINE_int32(chunk_size, 1000000, "Number of records per chunk");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_int32(local_partition_default_size, 1000000, "default size for local partition used to store scan results");  //NOLINT
DEFINE_string(proffile, "", "Performance measurement result file.");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(noop_pregroup, false, "do nothing in the shuffle pregroup");  //NOLINT
DEFINE_bool(shuffle_uses_sorted_vector, false, "shuffle to use sorted vector instead of priority queue, this enables noop_pregroup as well");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_int64(key_modulo, -1, "key value integer is calculated based on the given modulo. Specify -1 to disable.");  //NOLINT
DEFINE_bool(aggregate_group, false, "whether the result group will be aggregated");  //NOLINT

namespace jogasaki::group_cli {

using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::exchange;
using namespace jogasaki::executor::exchange::group;
using namespace jogasaki::scheduler;

std::shared_ptr<meta::record_meta> test_record_meta() {
    return std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                    meta::field_type(takatori::util::enum_tag<meta::field_type_kind::int8>),
                    meta::field_type(takatori::util::enum_tag<meta::field_type_kind::float8>),
            },
            boost::dynamic_bitset<std::uint64_t>{std::string("00")});
}

static int run(params& s, std::shared_ptr<configuration> cfg) {
    auto meta = test_record_meta();
    auto info = std::make_shared<group_info>(meta, std::vector<std::size_t>{0});

    auto channel = std::make_shared<class channel>();
    auto compiler_context = std::make_shared<plan::compiler_context>();
    auto context = std::make_shared<request_context>(channel, cfg, compiler_context);

    common::graph g{*context};
    auto& scan = g.emplace<producer_process>(meta, s);
    auto& xch = g.emplace<group::step>(info, meta::variable_order{}, meta::variable_order{});
    auto& emit = g.emplace<consumer_process>(info->group_meta(), s);
    auto& dvr = g.emplace<deliver::step>();
    scan >> xch;
    xch >> emit;
    emit >> dvr;

    dag_controller dc{std::move(cfg)};
    dc.schedule(g);
    return 0;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("group cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("group cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    jogasaki::group_cli::params s{};
    auto cfg = std::make_shared<jogasaki::configuration>();
    cfg->single_thread(!FLAGS_use_multithread);
    cfg->thread_pool_size(FLAGS_thread_pool_size);

    s.upstream_partitions_ = FLAGS_upstream_partitions;
    s.downstream_partitions_ = FLAGS_downstream_partitions;
    s.records_per_upstream_partition_ = FLAGS_records_per_partition;
    s.key_modulo_ = FLAGS_key_modulo;
    s.aggregate_group_ = FLAGS_aggregate_group;

    cfg->core_affinity(FLAGS_core_affinity);
    cfg->initial_core(FLAGS_initial_core);
    cfg->assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);
    cfg->noop_pregroup(FLAGS_noop_pregroup);

    if (FLAGS_shuffle_uses_sorted_vector) {
        cfg->use_sorted_vector(true);
        cfg->noop_pregroup(true);
    }

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
        jogasaki::group_cli::run(s, cfg);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    jogasaki::common_cli::dump_perf_info();

    return 0;
}
