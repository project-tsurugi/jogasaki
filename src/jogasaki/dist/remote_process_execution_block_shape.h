/*
 * Copyright 2018-2026 Project Tsurugi.
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



/**
 * @file
 * @brief validate the currently supported shape of a remote execution block.
 * @note Supports one scan/find source followed by filter/project and a single output.
 * @todo Extend eligibility together with graph restoration, result binding, and termination tests.
 */
#include <cstddef>

#include <takatori/relation/graph.h>

namespace jogasaki::dist {

enum class remote_process_source_kind {
    scan,
    find,
};

struct remote_process_execution_block_shape {
    bool eligible{};
    remote_process_source_kind source{};
    std::size_t intermediate_count{};
};

/**
 * @brief inspects whether the process graph matches the remote execution remote execution block rule.
 * @details The accepted shape is `[scan|find] -> [filter|project]* -> offer`.
 *     This is an intentionally narrow early rule, not the fundamental remote
 *     execution validity definition. Branches, disconnected nodes, missing offer,
 *     and other operators are rejected by the current execution block.
 */
[[nodiscard]] remote_process_execution_block_shape inspect_remote_process_execution_block_shape(
    takatori::relation::graph_type const& graph);

[[nodiscard]] bool is_remote_process_execution_block_shape(
    takatori::relation::graph_type const& graph);

} // namespace jogasaki::dist
