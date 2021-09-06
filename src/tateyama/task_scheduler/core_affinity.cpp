/*
 * Copyright 2018-2020 tsurugi project.
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
#include "core_affinity.h"

#include <boost/thread/thread.hpp>

#include <numa.h>

#include <tateyama/api/task_scheduler/task_scheduler_cfg.h>

namespace tateyama::task_scheduler {

bool thread_core_affinity(std::size_t cpu, bool uniform_on_nodes, std::size_t force_numa_node) {
    if (force_numa_node != numa_node_unspecified) {
        return 0 == numa_run_on_node(force_numa_node);
    }
    if (uniform_on_nodes) {
        auto nodes = numa_node_count();
        return 0 == numa_run_on_node(static_cast<int>(cpu % nodes));
    }
    pthread_t x = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);  //NOLINT
    return 0 == ::pthread_setaffinity_np(x, sizeof(cpu_set_t), &cpuset);
}

void setup_core_affinity(std::size_t id, api::task_scheduler::task_scheduler_cfg const* cfg) {
    if (! cfg) return;
    auto initial = cfg->initial_core();
    auto core = id+initial;
    bool assign_numa_nodes_uniformly = cfg->assign_numa_nodes_uniformly();
    bool core_affinity = cfg->core_affinity();
    std::size_t force_numa_node = cfg->force_numa_node();
    if (core_affinity || assign_numa_nodes_uniformly || force_numa_node != numa_node_unspecified) {
        thread_core_affinity(core, assign_numa_nodes_uniformly, force_numa_node);
    }
}

std::size_t numa_node_count() {
    static std::size_t nodes = numa_max_node()+1;
    return nodes;
}

}

