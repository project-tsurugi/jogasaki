/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <cstdint>
#include <type_traits>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <boost/move/utility_core.hpp>

#include <takatori/util/optional_ptr.h>
#include <yugawara/analyzer/block.h>
#include <yugawara/analyzer/block_algorithm.h>
#include <yugawara/analyzer/block_builder.h>
#include <yugawara/analyzer/variable_liveness_analyzer.h>
#include <yugawara/analyzer/variable_liveness_info.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/validation.h>

namespace jogasaki::executor::process::impl {

std::size_t value_info::value_offset() const noexcept {
    return value_offset_;
}

std::size_t value_info::nullity_offset() const noexcept {
    return nullity_offset_;
}

std::size_t value_info::index() const noexcept {
    return index_;
}

std::optional<std::size_t> value_info::block_index() const noexcept {
    return block_index_;
}

variable_table_info::variable_table_info(
    variable_table_info::entity_type map,
    maybe_shared_ptr<meta::record_meta> meta,
    variable_table_info const* parent
) :
    map_(std::move(map)),
    meta_(std::move(meta)),
    parent_(parent)
{
    // currently assuming any stream variables are nullable for now
    utils::assert_all_fields_nullable(*meta_);
}

static variable_table_info::entity_type from_indices(
    variable_table_info::variable_indices const& indices,
    maybe_shared_ptr<meta::record_meta> const& meta,
    std::optional<std::size_t> block_index
) {
    variable_table_info::entity_type map{};
    for(auto&& [v, i] : indices) {
        map[v] = value_info{meta->value_offset(i), meta->nullity_offset(i), i, block_index};
    }
    return map;
}

variable_table_info::variable_table_info(
    variable_indices const& indices,
    maybe_shared_ptr<meta::record_meta> meta
) :
    map_(from_indices(indices, meta, std::nullopt)),  // TODO pass block index is needed
    meta_(std::move(meta))
{
    // currently assuming any stream variables are nullable for now
    utils::assert_all_fields_nullable(*meta_);
}

maybe_shared_ptr<meta::record_meta> const& variable_table_info::meta() const noexcept {
    return meta_;
}

value_info const& variable_table_info::at(variable_table_info::variable const& var) const {
    if (auto it = map_.find(var); it != map_.end()) {
        return it->second;
    }
    if (parent_) {
        return parent_->at(var);
    }
    throw std::out_of_range{"variable not found in variable_table_info"};
}

bool variable_table_info::exists(variable_table_info::variable const& var) const {
    if (map_.count(var) != 0) return true;
    if (parent_) return parent_->exists(var);
    return false;
}

bool variable_table_info::exists_local(variable_table_info::variable const& var) const {
    return map_.count(var) != 0;
}

variable_table_info::variable_table_info(
    variable_indices const& indices,
    std::unordered_map<std::string, takatori::descriptor::variable> const& names,
    maybe_shared_ptr<meta::record_meta> meta
) :
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

static std::size_t count_blocks(yugawara::analyzer::block const& blk) {
    std::size_t n = 1;
    for (auto& ds : blk.downstreams()) {
        n += count_blocks(ds);
    }
    return n;
}

static void build_block(
    yugawara::analyzer::block const& blk,
    yugawara::analyzer::variable_liveness_analyzer& analyzer,
    yugawara::compiled_info const& info,
    variable_table_info const* parent_ptr,
    variables_info_list& entity,
    block_indices& indices
) {
    std::size_t current_idx = entity.size();

    auto&& liveness = analyzer.inspect(blk);
    auto& killed = liveness.kill();
    std::unordered_map<takatori::descriptor::variable, std::size_t> var_indices{};
    std::vector<meta::field_type> fields{};
    std::vector<takatori::descriptor::variable> variables{};

    fields.reserve(liveness.define().size());
    for (auto& v : liveness.define()) {
        if (killed.count(v) == 0) {
            fields.emplace_back(utils::type_for(info, v));
            variables.emplace_back(v);
        }
    }
    boost::dynamic_bitset<std::uint64_t> nullability{};
    nullability.resize(fields.size(), true); // currently stream variables are all nullable
    auto meta = std::make_shared<meta::record_meta>(std::move(fields), std::move(nullability));
    assert_with_exception(meta->field_count() == variables.size(), meta->field_count(), variables.size());
    for (std::size_t i = 0, n = meta->field_count(); i < n; ++i) {
        var_indices[variables[i]] = i;
    }

    entity.emplace_back(from_indices(var_indices, meta, current_idx), meta, parent_ptr);

    for (auto& e : blk) {
        indices[&e] = current_idx;
    }

    // parent_ptr is stable because entity was pre-reserved before build_block is first called
    for (auto& ds : blk.downstreams()) {
        build_block(ds, analyzer, info, &entity[current_idx], entity, indices);
    }
}

std::pair<std::shared_ptr<variables_info_list>, std::shared_ptr<block_indices>> create_block_variables_definition(
    relation::graph_type const& relations,
    yugawara::compiled_info const& info
) {
    auto bg = yugawara::analyzer::block_builder::build(const_cast<relation::graph_type&>(relations)); // work-around
    yugawara::analyzer::variable_liveness_analyzer analyzer { bg };

    auto head = yugawara::analyzer::find_unique_head(bg);
    if (!head) {
        fail_with_exception();
    }

    auto entity = std::make_shared<variables_info_list>();
    auto indices = std::make_shared<block_indices>();
    entity->reserve(count_blocks(*head));

    build_block(*head, analyzer, info, nullptr, *entity, *indices);

    return {std::move(entity), std::move(indices)};
}

}


