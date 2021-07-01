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
#include <chrono>

#include <glog/logging.h>

#include <tateyama/basic_task.h>
#include <tateyama/task_scheduler.h>
#include <tateyama/task_scheduler_cfg.h>
#include "utils.h"

DEFINE_int64(duration, 5000, "Run duration in milli-seconds");  //NOLINT
DEFINE_int32(thread_count, 10, "Number of threads");  //NOLINT
DEFINE_bool(core_affinity, true, "Whether threads are assigned to cores");  //NOLINT
DEFINE_int32(initial_core, 1, "initial core number, that the bunch of cores assignment begins with");  //NOLINT
DEFINE_bool(minimum, false, "run with minimum amount of data");  //NOLINT
DEFINE_bool(assign_numa_nodes_uniformly, true, "assign cores uniformly on all numa nodes - setting true automatically sets core_affinity=true");  //NOLINT
DEFINE_bool(debug, false, "debug mode");  //NOLINT

namespace tateyama::task_scheduler_cli {

using namespace std::string_literals;
using namespace std::string_view_literals;

using namespace tateyama::impl;

using clock = std::chrono::high_resolution_clock;

class test_task;
class test_task2;
using task = basic_task<test_task, test_task2>;

class cache_align test_task2 {
public:
    test_task2() = default;

    test_task2(
        tateyama::task_scheduler_cfg const& cfg,
        tateyama::task_scheduler<task>& scheduler,
        std::size_t generation
    ) :
        cfg_(std::addressof(cfg)),
        scheduler_(std::addressof(scheduler)),
        generation_(generation)
    {}

    void operator()(context& ctx) {
        // do nothing
        (void)ctx;
    }

    tateyama::task_scheduler_cfg const* cfg_{};
    tateyama::task_scheduler<task>* scheduler_{};
    std::size_t generation_{};
};

class cache_align test_task {
public:
    test_task() = default;

    test_task(
        tateyama::task_scheduler_cfg const& cfg,
        tateyama::task_scheduler<task>& scheduler,
        std::size_t generation
    ) :
        cfg_(std::addressof(cfg)),
        scheduler_(std::addressof(scheduler)),
        generation_(generation)
    {}

    void operator()(context& ctx) {
        scheduler_->schedule_at(task{test_task{*cfg_, *scheduler_, generation_+1}}, ctx.index());
    }

    tateyama::task_scheduler_cfg const* cfg_{};
    tateyama::task_scheduler<task>* scheduler_{};
    std::size_t generation_{};
};

using queue = basic_queue<task>;

bool fill_from_flags(
    task_scheduler_cfg& cfg,
    std::string const& str = {}
) {
    gflags::FlagSaver saver{};
    if (! str.empty()) {
        if(! gflags::ReadFlagsFromString(str, "", false)) {
            std::cerr << "parsing options failed" << std::endl;
        }
    }

    cfg.core_affinity(FLAGS_core_affinity);
    cfg.initial_core(FLAGS_initial_core);
    cfg.assign_numa_nodes_uniformly(FLAGS_assign_numa_nodes_uniformly);
    cfg.thread_count(FLAGS_thread_count);

    if (FLAGS_minimum) {
        cfg.thread_count(1);
        cfg.initial_core(1);
        cfg.core_affinity(false);
    }

    if (cfg.assign_numa_nodes_uniformly()) {
        cfg.core_affinity(true);
    }
    return true;
}

void show_result(
    std::vector<queue> const& queues,
    std::size_t duration_ms,
    std::vector<worker_stat> const& worker_stats,
    bool debug
) {
    std::size_t total_executions = 0;
    std::size_t index = 0;
    if (debug) {
        LOG(INFO) << "======= begin debug info =======";
    }
    for(auto&& q: const_cast<std::vector<queue>&>(queues)) {
        task t{};
        std::size_t queue_total = 0;
        while(q.try_pop(t)) {
            auto& tsk = std::get<0>(t.entity_);
            queue_total += tsk.generation_;
            total_executions += tsk.generation_;
        }
        if (debug) {
            LOG(INFO) << cwidth(2) << index << "-th queue executions " << format(queue_total) << " tasks";
        }
        ++index;
    }
    std::size_t idx = 0;
    for(auto&& w : worker_stats) {
        if (debug) {
            LOG(INFO) << cwidth(2) << idx << "-th thread executions " << format(w.count_)<< " tasks";
        }
        ++idx;
    }
    if (debug) {
        LOG(INFO) << "======= end debug info =======";
    }

    LOG(INFO) << "duration: " << format(duration_ms) << " ms";
    LOG(INFO) << "total executions: " << format(total_executions) << " tasks";
    LOG(INFO) << "total throughput: " << format((std::int64_t)((double)total_executions / duration_ms * 1000)) << " tasks/s";
    LOG(INFO) << "avg throughput: " << format((std::int64_t)((double)total_executions / queues.size() / duration_ms * 1000)) << " tasks/s/thread";
}

static int run(tateyama::task_scheduler_cfg const& cfg, bool debug, std::size_t duration) {
    LOG(INFO) << "configuration " << cfg;
    tateyama::task_scheduler<task> sched{cfg};
    for(std::size_t i=0, n=cfg.thread_count(); i < n; ++i) {
        sched.schedule_at(task{test_task{cfg, sched, 0}}, i);
    }
    sched.start();
    auto begin = clock::now();
    std::this_thread::sleep_for(std::chrono::milliseconds(duration));
    sched.stop();
    auto end = clock::now();
    auto duration_ms = std::chrono::duration_cast<clock::duration>(end-begin).count()/1000/1000;
    show_result(sched.queues(), duration_ms, sched.worker_stats(), debug);
    return 0;
}

}  // namespace

extern "C" int main(int argc, char* argv[]) {
    // ignore log level
    if (FLAGS_log_dir.empty()) {
        FLAGS_logtostderr = true;
    }
    google::InitGoogleLogging("task-scheduler cli");
    google::InstallFailureSignalHandler();
    gflags::SetUsageMessage("task-scheduler cli");
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc != 1) {
        gflags::ShowUsageWithFlags(argv[0]); // NOLINT
        return -1;
    }
    tateyama::task_scheduler_cfg cfg{};
    if(! tateyama::task_scheduler_cli::fill_from_flags(cfg)) return -1;
    try {
        tateyama::task_scheduler_cli::run(cfg, FLAGS_debug, FLAGS_duration);  // NOLINT
    } catch (std::exception& e) {
        std::cerr << e.what() << std::endl;
        return -1;
    }
    return 0;
}
