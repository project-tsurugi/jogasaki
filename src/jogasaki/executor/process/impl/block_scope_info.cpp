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
#include "block_scope_info.h"

#include <cassert>

#include <takatori/relation/expression.h>
#include <takatori/graph/graph.h>
#include <takatori/util/fail.h>
#include <yugawara/analyzer/block_builder.h>
#include <yugawara/analyzer/block_algorithm.h>
#include <yugawara/analyzer/variable_liveness_analyzer.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/field_types.h>
#include "variable_value_map.h"

namespace jogasaki::executor::process::impl {

using takatori::util::fail;

block_scope_info::block_scope_info(
    std::unique_ptr<variable_value_map> value_map,
    maybe_shared_ptr<meta::record_meta> meta
) noexcept :
    value_map_(std::move(value_map)),
    meta_(std::move(meta))
{}

variable_value_map& block_scope_info::value_map() const noexcept {
    return *value_map_;
}

maybe_shared_ptr<meta::record_meta> const& block_scope_info::meta() const noexcept {
    return meta_;
}

std::pair<scopes_info, scope_indices>
create_scopes_info(relation::graph_type &relations, const yugawara::compiled_info &info) {
    // analyze variables liveness
    // for each basic block, define a block_scope region with
    // result fields + defined fields (except killed in the same basic block)
    auto bg = yugawara::analyzer::block_builder::build(relations);
    yugawara::analyzer::variable_liveness_analyzer analyzer { bg };
    std::size_t block_index = 0;

    // FIXME support multiple blocks
    auto b0 = find_unique_head(bg);
    if (!b0) {
        // TODO multiple heads supported?
        fail();
    }
    auto&& n0 = analyzer.inspect(*b0);

    auto& killed = n0.kill();
    std::unordered_map<takatori::descriptor::variable, value_info> map{};
    std::vector<meta::field_type> fields{};
    std::vector<takatori::descriptor::variable> variables{};

    fields.reserve(n0.define().size());
    for(auto& v : n0.define()) {
        if (killed.count(v) == 0) {
            fields.emplace_back(utils::type_for(info, v));
            map[v] = value_info{};
            variables.emplace_back(v);
        }
    }
    boost::dynamic_bitset<std::uint64_t> nullability{};

    nullability.resize(fields.size()); // TODO fetch nullability
    auto meta = std::make_shared<meta::record_meta>(std::move(fields), std::move(nullability));
    assert(meta->field_count() == variables.size()); //NOLINT
    for(std::size_t i=0, n = meta->field_count(); i < n; ++i) {
        auto& v = variables[i];
        map[v] = value_info{meta->value_offset(i), meta->nullity_offset(i)};
    }

    scopes_info entity{};
    scope_indices indices{};

    entity.emplace_back(
        std::make_unique<variable_value_map>(std::move(map)),
        meta);

    for(auto&& e : *b0) {
        indices[&e] = block_index;
    }
    ++block_index;
    return {std::move(entity), std::move(indices)};
}

}


