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
#include "variable_order.h"

#include <boost/assert.hpp>

#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>

namespace jogasaki::meta {

using takatori::descriptor::variable;

variable_order::variable_order(
    variable_ordering_enum_tag_t<variable_ordering_kind::flat_record>,
    sequence_view<variable const> columns
) :
    record_or_key_(columns.begin(), columns.end())
{
    fill_flat_record(entity_, columns);
}

variable_order::variable_order(
    variable_ordering_enum_tag_t<variable_ordering_kind::flat_record_from_keys_values>,
    sequence_view<variable const> keys,
    sequence_view<variable const > values
) :
    record_or_key_(keys.begin(), keys.end())
{
    record_or_key_.insert(record_or_key_.end(), values.begin(), values.end());
    entity_type keys_order{};
    entity_type values_order{};
    fill_flat_record(keys_order, keys);
    fill_flat_record(values_order, values, keys.size());
    entity_.merge(keys_order);
    entity_.merge(values_order);
}

variable_order::variable_order(
    variable_ordering_enum_tag_t<variable_ordering_kind::group_from_keys>,
    sequence_view<variable const> columns,
    sequence_view<variable const> group_keys
) :
    for_group_(true)
{
    entity_type columns_membership{};
    fill_flat_record(columns_membership, columns);
    record_or_key_.reserve(group_keys.size());
    for(auto&& c : group_keys) {
        if (columns_membership.count(c) != 0) {
            record_or_key_.emplace_back(c);
        }
    }
    entity_type keys_order{};
    fill_flat_record(keys_order, record_or_key_);

    value_.reserve(columns.size()); // can be larger than necessary - group_keys might not appear in columns
    for(auto&& column : columns) {
        if (keys_order.count(column) == 0) {
            value_.emplace_back(column);
        }
    }
    entity_type values_order{};
    fill_flat_record(values_order, value_);

    entity_.reserve(columns.size());
    for(auto&& k : group_keys) {
        if (columns_membership.count(k) == 0) continue;
        entity_.emplace(k, keys_order[k]);
        key_bool_.emplace(k, true);
    }
    for(auto&& v : value_) {
        entity_.emplace(v, values_order[v]);
        key_bool_.emplace(v, false);
    }
}

variable_order::index_type variable_order::index(variable const& var) const {
    return entity_.at(var);
}

std::pair<variable_order::index_type, bool> variable_order::key_value_index(variable const& var) const {
    BOOST_ASSERT(for_group_);  //NOLINT
    return { entity_.at(var), key_bool_.at(var) };
}

bool variable_order::for_group() const noexcept {
    return for_group_;
}

bool variable_order::is_key(variable const&var) const {
    BOOST_ASSERT(for_group_);  //NOLINT
    return key_bool_.at(var);
}

std::size_t variable_order::size() const noexcept {
    return entity_.size();
}

std::size_t variable_order::key_count() const noexcept {
    BOOST_ASSERT(for_group_);  //NOLINT
    std::size_t ret = 0;
    for(auto&& k : key_bool_) {
        if (k.second) {
            ++ret;
        }
    }
    return ret;
}

void variable_order::fill_flat_record(
    variable_order::entity_type &entity,
    sequence_view<variable const> columns,
    std::size_t begin_offset
) {
    // oredering arbitrarily for now
    //TODO order shorter types first, alphabetically
    auto sz = columns.size();
    entity.reserve(sz);
    for(std::size_t i=0; i < sz; ++i) {
        entity.emplace(columns[i], i+begin_offset);
    }
}

variable_order::const_iterator variable_order::begin() const noexcept {
    return record_or_key_.begin();
}

variable_order::const_iterator variable_order::end() const noexcept {
    return record_or_key_.end();
}

variable_order::const_iterator variable_order::value_begin() const noexcept {
    BOOST_ASSERT(for_group_);  //NOLINT
    return value_.begin();
}

variable_order::const_iterator variable_order::value_end() const noexcept {
    BOOST_ASSERT(for_group_);  //NOLINT
    return value_.end();
}

}

