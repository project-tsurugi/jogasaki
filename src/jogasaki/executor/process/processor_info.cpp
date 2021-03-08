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
#include "processor_info.h"

#include <takatori/graph/graph.h>
#include <takatori/plan/graph.h>
#include <yugawara/compiler_result.h>
#include <jogasaki/executor/process/impl/block_scope_info.h>

namespace jogasaki::executor::process {

namespace relation = takatori::relation;

processor_details::processor_details(
    bool has_scan_operator,
    bool has_emit_operator,
    bool has_find_operator,
    bool has_write_operations
) :
    has_scan_operator_(has_scan_operator),
    has_emit_operator_(has_emit_operator),
    has_find_operator_(has_find_operator),
    has_write_operations_(has_write_operations)
{}

bool processor_details::has_scan_operator() const noexcept {
    return has_scan_operator_;
}

bool processor_details::has_emit_operator() const noexcept {
    return has_emit_operator_;
}

bool processor_details::has_find_operator() const noexcept {
    return has_find_operator_;
}

bool processor_details::has_write_operations() const noexcept {
    return has_write_operations_;
}


processor_info::processor_info(
    relation::graph_type const& relations,
    yugawara::compiled_info info
) :
    relations_(std::addressof(relations)),
    info_(std::move(info)),
    details_(create_details())
{
    auto&& p = impl::create_scopes_info(*relations_, info_);
    scopes_info_ = std::move(p.first);
    scope_indices_ = std::move(p.second);
}

relation::graph_type const& processor_info::relations() const noexcept {
    return *relations_;
}

yugawara::compiled_info const& processor_info::compiled_info() const noexcept {
    return info_;
}

impl::scopes_info const& processor_info::scopes_info() const noexcept {
    return scopes_info_;
}

impl::scope_indices const& processor_info::scope_indices() const noexcept {
    return scope_indices_;
}

processor_details const& processor_info::details() const noexcept {
    return details_;
}

processor_details processor_info::create_details() {
    relation::graph_type g;
    bool has_scan_operator = false;
    bool has_emit_operator = false;
    bool has_find_operator = false;
    bool has_write_operator = false;
    using kind = relation::expression_kind;
    takatori::relation::sort_from_upstream(*relations_, [&](relation::expression const& node) {
        switch(node.kind()) {
            case kind::scan:
                has_scan_operator = true;
                break;
            case kind::emit:
                has_emit_operator = true;
                break;
            case kind::find:
                has_find_operator = true;
                break;
            case kind::write:
                has_write_operator = true;
                break;
            default:
                break;
        }
    });
    return {
        has_scan_operator,
        has_emit_operator,
        has_find_operator,
        has_write_operator
    };
}

}


