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
#include <memory>

#include <takatori/relation/graph.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/executor/process/step.h>

namespace jogasaki::executor::process {
// Graph and compiled_info must outlive the returned processor, as for ordinary SQL.
[[nodiscard]] std::shared_ptr<processor_info> prepare_processor(
    takatori::relation::graph_type const& graph, yugawara::compiled_info const& info,
    variable_table const* host_variables = nullptr,
    takatori::util::maybe_shared_ptr<impl::variables_info_list const> variables = {},
    takatori::util::maybe_shared_ptr<impl::block_indices const> blocks = {});

// The caller resolves ordinary exchanges or external relation IDs into slot indices.
// Runtime objects and I/O metadata are then constructed by the same code for both paths.
[[nodiscard]] step prepare_process(std::shared_ptr<processor_info> processor,
    std::shared_ptr<relation_io_map> relations);
void bind_process_io(step& process, std::shared_ptr<io_exchange_map> exchanges);
} // namespace jogasaki::executor::process
