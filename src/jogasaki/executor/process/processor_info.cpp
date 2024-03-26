/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <utility>

#include <takatori/relation/expression.h>
#include <takatori/relation/write.h>
#include <takatori/relation/write_kind.h>
#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>

namespace jogasaki::executor::process {

using takatori::util::unsafe_downcast;
namespace relation = takatori::relation;

processor_details::processor_details(
    bool has_scan_operator,
    bool has_emit_operator,
    bool has_find_operator,
    bool has_join_find_or_scan_operator,
    bool has_write_operations,
    bool write_for_update
) :
    has_scan_operator_(has_scan_operator),
    has_emit_operator_(has_emit_operator),
    has_find_operator_(has_find_operator),
    has_join_find_or_scan_operator_(has_join_find_or_scan_operator),
    has_write_operations_(has_write_operations),
    write_for_update_(write_for_update)
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

bool processor_details::write_for_update() const noexcept {
    return write_for_update_;
}

bool processor_details::has_join_find_or_scan_operator() const noexcept {
    return has_join_find_or_scan_operator_;
}

processor_info::processor_info(
    relation::graph_type const& relations,
    yugawara::compiled_info info,
    maybe_shared_ptr<impl::variables_info_list const> vars_info_list,
    maybe_shared_ptr<impl::block_indices const> block_inds,
    variable_table const* host_variables
) :
    relations_(std::addressof(relations)),
    info_(std::move(info)),
    vars_info_list_(std::move(vars_info_list)),
    block_indices_(std::move(block_inds)),
    details_(create_details()),
    host_variables_(host_variables)
{}

processor_info::processor_info(
    relation::graph_type const& relations,
    yugawara::compiled_info info,
    variable_table const* host_variables
) :
    relations_(std::addressof(relations)),
    info_(std::move(info)),
    details_(create_details()),
    host_variables_(host_variables)
{
    auto&& p = impl::create_block_variables_definition(*relations_, info_);
    vars_info_list_ = std::move(p.first);
    block_indices_ = std::move(p.second);
}

relation::graph_type const& processor_info::relations() const noexcept {
    return *relations_;
}

yugawara::compiled_info const& processor_info::compiled_info() const noexcept {
    return info_;
}

impl::variables_info_list const& processor_info::vars_info_list() const noexcept {
    return *vars_info_list_;
}

impl::block_indices const& processor_info::block_indices() const noexcept {
    return *block_indices_;
}

processor_details const& processor_info::details() const noexcept {
    return details_;
}

processor_details processor_info::create_details() {
    relation::graph_type g;
    bool has_scan_operator = false;
    bool has_emit_operator = false;
    bool has_find_operator = false;
    bool has_join_find_or_scan_operator = false;
    bool has_write_operator = false;
    bool write_for_update = false;
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
                write_for_update =
                    unsafe_downcast<relation::write const&>(node).operator_kind() == relation::write_kind::update;
                break;
            case kind::join_find:
                has_join_find_or_scan_operator = true;
                break;
            case kind::join_scan:
                has_join_find_or_scan_operator = true;
                break;
            default:
                break;
        }
    });
    return {
        has_scan_operator,
        has_emit_operator,
        has_find_operator,
        has_join_find_or_scan_operator,
        has_write_operator,
        write_for_update
    };
}

}


