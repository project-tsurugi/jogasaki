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

namespace jogasaki::utils {

/**
 * @brief set the core affinity of current thread
 * @param cpu the core number associated with current thread
 * @param uniform_on_nodes indicate whether the cpu number should be translated to node number to distribute uniformly
 * @return true when successful
 * @return false otherwise
 */
bool thread_core_affinity(std::size_t cpu, bool uniform_on_nodes, std::size_t force_numa_node) {
    if (force_numa_node != numa_node_unspecified) {
        return 0 == numa_run_on_node(force_numa_node);
    }
    if (uniform_on_nodes) {
        static std::size_t nodes = numa_max_node()+1;
        return 0 == numa_run_on_node(static_cast<int>(cpu % nodes));
    }
    pthread_t x = pthread_self();
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(cpu, &cpuset);  //NOLINT
    return 0 == ::pthread_setaffinity_np(x, sizeof(cpu_set_t), &cpuset);
}

}

