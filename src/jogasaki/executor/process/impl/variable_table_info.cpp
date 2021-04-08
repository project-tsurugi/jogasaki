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
#include "variable_table_info.h"

#include <takatori/util/fail.h>
#include <yugawara/analyzer/block_builder.h>
#include <yugawara/analyzer/variable_liveness_analyzer.h>
#include <yugawara/analyzer/block_algorithm.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/validation.h>

namespace jogasaki::executor::process::impl {

using takatori::util::fail;

std::size_t value_info::value_offset() const noexcept {
    return value_offset_;
}

std::size_t value_info::nullity_offset() const noexcept {
    return nullity_offset_;
}

variable_table_info::variable_table_info(
    variable_table_info::entity_type map,
    maybe_shared_ptr<meta::record_meta> meta
) noexcept :
    map_(std::move(map)),
    meta_(std::move(meta))
{
    // currently assuming any stream variables are nullable for now
    utils::assert_all_fields_nullable(*meta_);
}

variable_table_info::entity_type from_indices(
    variable_table_info::variable_indices const& indices,
    maybe_shared_ptr<meta::record_meta> const& meta
) {
    variable_table_info::entity_type map{};
    for(auto&& [v, i] : indices) {
        map[v] = value_info{meta->value_offset(i), meta->nullity_offset(i)};
    }
    return map;
}

variable_table_info::variable_table_info(
    variable_indices const& indices,
    maybe_shared_ptr<meta::record_meta> meta
) noexcept :
    map_(from_indices(indices, meta)),
    meta_(std::move(meta))
{
    // currently assuming any stream variables are nullable for now
    utils::assert_all_fields_nullable(*meta_);
}

maybe_shared_ptr<meta::record_meta> const& variable_table_info::meta() const noexcept {
    return meta_;
}

value_info const& variable_table_info::at(variable_table_info::variable const& var) const {
    return map_.at(var);
}

bool variable_table_info::exists(variable_table_info::variable const& var) const {
    return map_.count(var) != 0;
}

variable_table_info::variable_table_info(
    variable_indices const& indices,
    std::unordered_map<std::string, takatori::descriptor::variable> const& names,
    maybe_shared_ptr<meta::record_meta> meta
) noexcept:
    variable_table_info(indices, std::move(meta))
{
    for(auto& [name, v] : names) {
        add(name, v);
    }
}

value_info const& variable_table_info::at(std::string_view name) const {
    return named_map_.at(std::string(name));
}

void variable_table_info::add(std::string_view name, variable_table_info::variable const& var) {
    named_map_[std::string(name)] = map_[var];
}

bool variable_table_info::exists(std::string_view name) const {
    return named_map_.count(std::string(name)) != 0;
}

std::pair<std::shared_ptr<variables_info_list>, std::shared_ptr<block_indices>> create_block_variables_definition(
    relation::graph_type const& relations,
    yugawara::compiled_info const& info
) {
    // analyze variables liveness
    // for each basic block, define a variable_table region with
    // result fields + defined fields (except killed in the same basic block)
    auto bg = yugawara::analyzer::block_builder::build(const_cast<relation::graph_type&>(relations)); // work-around
    yugawara::analyzer::variable_liveness_analyzer analyzer { bg };
    std::size_t block_index = 0;

    // FIXME support multiple blocks
    auto b0 = yugawara::analyzer::find_unique_head(bg);
    if (!b0) {
        fail();
    }
    auto&& n0 = analyzer.inspect(*b0);
    auto& killed = n0.kill();
    std::unordered_map<takatori::descriptor::variable, std::size_t> map{};
    std::vector<meta::field_type> fields{};
    std::vector<takatori::descriptor::variable> variables{};

    fields.reserve(n0.define().size());
    for(auto& v : n0.define()) {
        if (killed.count(v) == 0) {
            fields.emplace_back(utils::type_for(info, v));
            variables.emplace_back(v);
        }
    }
    boost::dynamic_bitset<std::uint64_t> nullability{};
    nullability.resize(fields.size(), true); // currently stream variables are all nullable
    auto meta = std::make_shared<meta::record_meta>(std::move(fields), std::move(nullability));
    BOOST_ASSERT(meta->field_count() == variables.size()); //NOLINT
    for(std::size_t i=0, n = meta->field_count(); i < n; ++i) {
        auto& v = variables[i];
        map[v] = i;
    }

    auto entity = std::make_shared<variables_info_list>();
    auto indices = std::make_shared<block_indices>();
    entity->emplace_back(std::move(map), meta);
    for(auto&& e : *b0) {
        indices->operator[](&e) = block_index;
    }
    ++block_index;
    return {std::move(entity), std::move(indices)};
}

}


