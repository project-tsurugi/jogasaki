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

#include <executor/common/graph.h>
#include <executor/common/port.h>
#include <scheduler/dag_controller.h>
#include <executor/exchange/deliver/step.h>
#include <executor/exchange/group/shuffle_info.h>
#include "producer_process.h"
#include "consumer_process.h"
#include "context.h"
#include "constants.h"
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

static int run(context& s) {
    auto meta = test_record_meta();
    auto info = std::make_shared<shuffle_info>(meta, std::vector<std::size_t>{0});
    auto g = std::make_unique<common::graph>();
    auto scan = std::make_unique<producer_process>(g.get(), meta, s);
    auto xch = std::make_unique<group::step>(info);
    auto emit = std::make_unique<consumer_process>(g.get(), info->group_meta(), s);
    auto dvr = std::make_unique<deliver::step>();
    *scan >> *xch;
    *xch >> *emit;
    *emit >> *dvr;
    // step id are assigned from 0 to 3
    g->insert(std::move(scan));
    g->insert(std::move(xch));
    g->insert(std::move(emit));
    g->insert(std::move(dvr));

    configuration cfg;
    cfg.thread_pool_size_ = s.thread_pool_size_;
    cfg.single_thread_task_scheduler_ = !s.use_multithread;
    cfg.default_process_partitions_ = s.downstream_partitions_;
    cfg.default_scan_process_partitions_ = s.upstream_partitions_;
    cfg.set_core_affinity_ = s.set_core_affinity_;
    cfg.initial_core_ = s.initial_core_;
    dag_controller dc{&cfg};
    dc.schedule(*g);
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

    jogasaki::group_cli::context s{};
    s.use_multithread = FLAGS_use_multithread;
    s.thread_pool_size_ = FLAGS_thread_pool_size;
    s.upstream_partitions_ = FLAGS_upstream_partitions;
    s.downstream_partitions_ = FLAGS_downstream_partitions;
    s.records_per_upstream_partition_ = FLAGS_records_per_partition;
    s.initial_core_ = FLAGS_initial_core;
    s.set_core_affinity_ = FLAGS_core_affinity;

    if (FLAGS_minimum) {
        s.use_multithread = false;
        s.thread_pool_size_ = 1;
        s.upstream_partitions_ = 1;
        s.downstream_partitions_ = 1;
        s.records_per_upstream_partition_ = 1;
        s.initial_core_ = 1;
        s.set_core_affinity_ = false;
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
    watch->wrap(time_point_main_completed);
    LOG(INFO) << "prepare: total " << watch->duration(time_point_produce,time_point_prepare) << "ms, average " << watch->average_duration(time_point_produce,time_point_prepare) << "ms" ;
    LOG(INFO) << "produce: total " << watch->duration(time_point_pregroup,time_point_produce) << "ms, average " << watch->average_duration(time_point_pregroup,time_point_produce) << "ms" ;
    LOG(INFO) << "pregroup: total " << watch->duration(time_point_pregrouped, time_point_pregroup) << "ms, average " << watch->average_duration(time_point_pregrouped, time_point_pregroup) << "ms" ;
    LOG(INFO) << "transfer: total " << watch->duration(time_point_consume, time_point_pregrouped) << "ms, average " << watch->average_duration(time_point_consume, time_point_pregrouped) << "ms" ;
    LOG(INFO) << "consume: total " << watch->duration(time_point_consumed, time_point_consume) << "ms, average " << watch->average_duration(time_point_consumed, time_point_consume) << "ms" ;
    LOG(INFO) << "finish: total " << watch->duration(time_point_main_completed,time_point_consumed) << "ms, average " << watch->average_duration(time_point_main_completed,time_point_consumed) << "ms" ;
    return 0;
}

