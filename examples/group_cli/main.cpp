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
#include <jogasaki/executor/exchange/group/shuffle_info.h>
#include <jogasaki/constants.h>

#include "producer_process.h"
#include "consumer_process.h"
#include "params.h"
#include <cli_constants.h>

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
DEFINE_bool(assign_nume_nodes_uniformly, false, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT

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

static int run(params& s) {
    auto meta = test_record_meta();
    auto info = std::make_shared<shuffle_info>(meta, std::vector<std::size_t>{0});

    auto cfg = std::make_shared<configuration>();
    cfg->thread_pool_size(s.thread_pool_size_);
    cfg->single_thread(!s.use_multithread);
    cfg->default_partitions(s.downstream_partitions_);
    cfg->default_scan_process_partitions(s.upstream_partitions_);
    cfg->core_affinity(s.set_core_affinity_);
    cfg->initial_core(s.initial_core_);
    cfg->use_sorted_vector(s.use_sorted_vector_reader_);
    cfg->noop_pregroup(s.noop_pregroup_);
    cfg->assign_nume_nodes_uniformly(s.assign_nume_nodes_uniformly_);

    auto channel = std::make_shared<class channel>();
    auto context = std::make_shared<request_context>(channel, cfg);

    common::graph g{context};
    auto& scan = g.emplace<producer_process>(meta, s);
    auto& xch = g.emplace<group::step>(info);
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
    s.use_multithread = FLAGS_use_multithread;
    s.thread_pool_size_ = FLAGS_thread_pool_size;
    s.upstream_partitions_ = FLAGS_upstream_partitions;
    s.downstream_partitions_ = FLAGS_downstream_partitions;
    s.records_per_upstream_partition_ = FLAGS_records_per_partition;
    s.initial_core_ = FLAGS_initial_core;
    s.set_core_affinity_ = FLAGS_core_affinity;
    s.noop_pregroup_ = FLAGS_noop_pregroup;
    s.assign_nume_nodes_uniformly_ = FLAGS_assign_nume_nodes_uniformly;

    if (FLAGS_shuffle_uses_sorted_vector) {
        s.use_sorted_vector_reader_ = true;
        s.noop_pregroup_ = true;
    }

    if (FLAGS_minimum) {
        s.use_multithread = false;
        s.thread_pool_size_ = 1;
        s.upstream_partitions_ = 1;
        s.downstream_partitions_ = 1;
        s.records_per_upstream_partition_ = 1;
        s.initial_core_ = 1;
        s.set_core_affinity_ = false;
    }

    if (s.assign_nume_nodes_uniformly_) {
        s.set_core_affinity_ = true;
    }

    if (s.thread_pool_size_ < s.upstream_partitions_) {
        LOG(WARNING) << "thread pool size (" << s.thread_pool_size_ << ") is smaller than the number of upstream partitions(" << s.upstream_partitions_ << ") Not all of them are processed concurrently.";
    }
    if (s.thread_pool_size_ < s.downstream_partitions_) {
        LOG(WARNING) << "thread pool size (" << s.thread_pool_size_ << ") is smaller than the number of downstream partitions(" << s.downstream_partitions_ << ") Not all of them are processed concurrently.";
    }
    try {
        jogasaki::group_cli::run(s);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }

    auto& watch = s.watch_;
    using namespace jogasaki::group_cli;
    watch->set_point(time_point_main_completed);

    LOG(INFO) << "prepare_total\t" << watch->duration(time_point_prepare, time_point_produce) << " ms" ;
    auto results = watch->durations(time_point_prepare, time_point_produce);
    for(auto r : *results.get()) {
      LOG(INFO) << "prepare\t" << r << " ms" ;
    }

    LOG(INFO) << "product_total\t" << watch->duration(time_point_produce, time_point_produced) << " ms" ;
    results = watch->durations(time_point_produce, time_point_produced);
    for(auto r : *results.get()) {
      LOG(INFO) << "produce\t" << r << " ms" ;
    }

    LOG(INFO) << "transfer_total " << watch->duration(time_point_produced, time_point_consume, true) << " ms" ;

    LOG(INFO) << "consume_total\t" << watch->duration(time_point_consume, time_point_consumed) << " ms" ;
    results = watch->durations(time_point_consume, time_point_consumed);
    for(auto r : *results.get()) {
      LOG(INFO) << "consume\t" << r << " ms" ;
    }

    LOG(INFO) << "finish_total " << watch->duration(time_point_consumed, time_point_main_completed, true) << " ms" ;

    return 0;
}
