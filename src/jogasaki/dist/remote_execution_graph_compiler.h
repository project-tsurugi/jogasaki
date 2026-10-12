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
#include <string>

#include <takatori/relation/graph.h>
#include <yugawara/compiled_info.h>

namespace jogasaki::executor::process {
class processor_info;
}

namespace jogasaki::dist {

// Public result fields intentionally remain aggregate-accessible alongside the success query.
// NOLINTBEGIN(misc-non-private-member-variables-in-classes)
struct remote_execution_graph_compile_result {
    std::shared_ptr<yugawara::compiled_info> compiled_info{};
    std::shared_ptr<executor::process::processor_info> processor_info{};
    std::string error{};
    bool unsupported{};

    [[nodiscard]] explicit operator bool() const noexcept {
        return compiled_info != nullptr && processor_info != nullptr;
    }
};
// NOLINTEND(misc-non-private-member-variables-in-classes)

[[nodiscard]] remote_execution_graph_compile_result
compile_remote_execution_graph(takatori::relation::graph_type const& graph);

} // namespace jogasaki::dist
