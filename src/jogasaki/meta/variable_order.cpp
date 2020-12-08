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
#include "variable_order.h"

namespace jogasaki::meta {

using takatori::util::fail;
using takatori::descriptor::variable;

variable_order::variable_order(variable_ordering_enum_tag_t<variable_ordering_kind::flat_record>,
    takatori::util::sequence_view<const variable> columns) {
    fill_flat_record(entity_, columns);
}

variable_order::variable_order(variable_ordering_enum_tag_t<variable_ordering_kind::flat_record_from_keys_values>,
    takatori::util::sequence_view<const variable> keys, takatori::util::sequence_view<const variable> values) {
    entity_type keys_order{};
    entity_type values_order{};
    fill_flat_record(keys_order, keys);
    fill_flat_record(values_order, values, keys.size());
    entity_.merge(keys_order);
    entity_.merge(values_order);
}

variable_order::variable_order(variable_ordering_enum_tag_t<variable_ordering_kind::group_from_keys>,
    takatori::util::sequence_view<const variable> columns, takatori::util::sequence_view<const variable> group_keys) :
    for_group_(true)
{
    entity_type columns_membership{};
    fill_flat_record(columns_membership, columns);
    std::vector<variable> exposed_keys{};
    exposed_keys.reserve(group_keys.size());
    for(auto&& c : group_keys) {
        if (columns_membership.count(c) != 0) {
            exposed_keys.emplace_back(c);
        }
    }
    entity_type keys_order{};
    fill_flat_record(keys_order, exposed_keys);

    std::vector<variable> values{};
    values.reserve(columns.size()); // can be larger than necessary - group_keys might not appear in columns
    for(auto&& column : columns) {
        if (keys_order.count(column) == 0) {
            values.emplace_back(column);
        }
    }
    entity_type values_order{};
    fill_flat_record(values_order, values);

    entity_.reserve(columns.size());
    for(auto&& k : group_keys) {
        if (columns_membership.count(k) == 0) continue;
        entity_.emplace(k, keys_order[k]);
        key_bool_.emplace(k, true);
    }
    for(auto&& v : values) {
        entity_.emplace(v, values_order[v]);
        key_bool_.emplace(v, false);
    }
}

variable_order::variable_index_type variable_order::index(const variable &var) const {
    return entity_.at(var);
}

std::pair<variable_order::variable_index_type, bool> variable_order::key_value_index(const variable &var) const {
    BOOST_ASSERT(for_group_);  //NOLINT
    return { entity_.at(var), key_bool_.at(var) };
}

bool variable_order::for_group() const noexcept {
    return for_group_;
}

bool variable_order::is_key(const variable &var) const {
    assert(for_group_);  //NOLINT
    return key_bool_.at(var);
}

std::size_t variable_order::size() const noexcept {
    return entity_.size();
}

void variable_order::fill_flat_record(variable_order::entity_type &entity,
    takatori::util::sequence_view<const variable> columns, std::size_t begin_offset) {
    // oredering arbitrarily for now
    //TODO order shorter types first, alphabetically
    auto sz = columns.size();
    entity.reserve(sz);
    for(std::size_t i=0; i < sz; ++i) {
        entity.emplace(columns[i], i+begin_offset);
    }
}
}

