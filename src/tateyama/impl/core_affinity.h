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
#pragma once

#include <boost/thread/thread.hpp>

namespace tateyama {
class task_scheduler_cfg;
}

namespace tateyama::impl {


constexpr std::size_t numa_node_unspecified = static_cast<std::size_t>(-1); // same constant on configuration.h

/**
 * @brief set the core affinity of current thread
 * @param cpu the core number associated with current thread
 * @param uniform_on_nodes indicate whether the cpu number should be translated to node number to distribute uniformly.
 * If true is specified for this parameter, the cpu parameter is ignored.
 * @param force_numa_node force running on the specified node. If other values than numa_node_unspecified are specified,
 * this parameter precedes the setting given by cpu or uniform_on_nodes fixing the running numa node.
 * @return true when successful
 * @return false otherwise
 */
bool thread_core_affinity(std::size_t cpu, bool uniform_on_nodes = false, std::size_t force_numa_node = numa_node_unspecified);

void setup_core_affinity(std::size_t id, task_scheduler_cfg const* cfg);

/**
 * @return the number of numa nodes
 */
std::size_t numa_node_count();

}

