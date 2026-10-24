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

#include "remote_process_execution_block_shape.h"

#include <memory>
#include <unordered_set>

#include <takatori/relation/expression.h>
#include <takatori/relation/expression_kind.h>

namespace jogasaki::dist {

namespace {

namespace relation = takatori::relation;

} // namespace

[[nodiscard]] static bool is_source(relation::expression_kind kind) noexcept {
    using kind_type = relation::expression_kind;
    return kind == kind_type::scan || kind == kind_type::find;
}

[[nodiscard]] static bool is_intermediate(relation::expression_kind kind) noexcept {
    using kind_type = relation::expression_kind;
    return kind == kind_type::filter || kind == kind_type::project;
}

[[nodiscard]] static remote_process_source_kind source_kind(relation::expression_kind kind) noexcept {
    return kind == relation::expression_kind::scan ? remote_process_source_kind::scan
                                                   : remote_process_source_kind::find;
}

[[nodiscard]] static relation::expression const* single_downstream(relation::expression const& node) {
    auto outputs = node.output_ports();
    if (outputs.size() != 1) {
        return {};
    }
    auto downstream = outputs[0].opposite();
    if (!downstream) {
        return {};
    }
    return std::addressof(downstream->owner());
}

remote_process_execution_block_shape inspect_remote_process_execution_block_shape(relation::graph_type const& graph) {
    if (graph.empty()) {
        return {};
    }

    relation::expression const* head{};
    std::size_t head_count{};
    relation::enumerate_top(graph, [&](relation::expression const& node) {
        head = std::addressof(node);
        ++head_count;
    });
    if (head_count != 1 || head == nullptr || !is_source(head->kind())) {
        return {};
    }

    remote_process_execution_block_shape result{
        true,
        source_kind(head->kind()),
        0,
    };
    std::unordered_set<relation::expression const*> visited{};
    relation::expression const* current = head;

    while (current != nullptr) {
        if (!visited.insert(current).second) {
            return {};
        }

        auto const kind = current->kind();
        if (kind == relation::expression_kind::offer) {
            result.eligible = visited.size() == graph.size();
            return result.eligible ? result : remote_process_execution_block_shape{};
        }

        if (current != head) {
            if (!is_intermediate(kind)) {
                return {};
            }
            ++result.intermediate_count;
        }

        current = single_downstream(*current);
    }

    return {};
}

bool is_remote_process_execution_block_shape(relation::graph_type const& graph) {
    return inspect_remote_process_execution_block_shape(graph).eligible;
}

} // namespace jogasaki::dist
