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
#include "shuffle_info.h"

#include <vector>
#include <set>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/constants.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/group_meta.h>

namespace jogasaki::executor::exchange::group {

using takatori::util::maybe_shared_ptr;

shuffle_info::shuffle_info(
    maybe_shared_ptr<meta::record_meta> record,
    std::vector<field_index_type> key_indices,
    std::vector<field_index_type> const& key_indices_for_sort,
    std::vector<ordering> const& key_ordering_for_sort
) :
    record_(std::move(record)),
    key_indices_(std::move(key_indices)),
    group_(std::make_shared<meta::group_meta>(
        from_keys(record_, key_indices_),
        create_value_meta(record_, key_indices_))),
    sort_key_(create_sort_key_meta(record_, key_indices_, key_indices_for_sort)),
    sort_key_ordering_(create_sort_key_ordering(key_indices_.size(), key_ordering_for_sort)),
    sort_compare_info_(*sort_key_, sort_key_ordering_)
{
    BOOST_ASSERT(key_indices_for_sort.size() == key_ordering_for_sort.size());  //NOLINT
}

accessor::record_ref shuffle_info::extract_key(accessor::record_ref record) const noexcept {
    return accessor::record_ref(record.data(), record_->record_size());
}

accessor::record_ref shuffle_info::extract_value(accessor::record_ref record) const noexcept {
    return accessor::record_ref(record.data(), record_->record_size());
}

accessor::record_ref shuffle_info::extract_sort_key(accessor::record_ref record) const noexcept {
    return accessor::record_ref(record.data(), record_->record_size());
}

sequence_view<ordering const> shuffle_info::sort_key_ordering() const noexcept {
    return sort_key_ordering_;
}

const maybe_shared_ptr<meta::record_meta> &shuffle_info::record_meta() const noexcept {
    return record_;
}

const maybe_shared_ptr<meta::record_meta> &shuffle_info::key_meta() const noexcept {
    return group_->key_shared();
}

const maybe_shared_ptr<meta::record_meta> &shuffle_info::value_meta() const noexcept {
    return group_->value_shared();
}

const maybe_shared_ptr<meta::record_meta> &shuffle_info::sort_key_meta() const noexcept {
    return sort_key_;
}

const maybe_shared_ptr<meta::group_meta> &shuffle_info::group_meta() const noexcept {
    return group_;
}

std::shared_ptr<meta::record_meta> shuffle_info::from_keys(
    maybe_shared_ptr<meta::record_meta> record,
    std::vector<std::size_t> const& indices
) {
    auto num = indices.size();
    meta::record_meta::fields_type fields{};
    meta::record_meta::nullability_type  nullables(num);
    meta::record_meta::value_offset_table_type value_offset_table{};
    meta::record_meta::nullity_offset_table_type nullity_offset_table{};
    fields.reserve(num);
    value_offset_table.reserve(num);
    nullity_offset_table.reserve(num);
    for(std::size_t i=0; i < num; ++i) {
        auto ind = indices[i];
        fields.emplace_back(record->at(ind));
        value_offset_table.emplace_back(record->value_offset(ind));
        nullity_offset_table.emplace_back(record->nullity_offset(ind));
        if (record->nullable(ind)) {
            nullables.set(i);
        }
    }
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        std::move(nullables),
        std::move(value_offset_table),
        std::move(nullity_offset_table),
        record->record_alignment(),
        record->record_size()
    );
}

std::shared_ptr<meta::record_meta> shuffle_info::create_value_meta(
    maybe_shared_ptr<meta::record_meta> record,
    std::vector<std::size_t> const& key_indices
) {
    std::size_t num = record_->field_count() - key_indices.size();
    std::set<std::size_t> indices{key_indices.begin(), key_indices.end()};
    std::vector<field_index_type> vec{};
    vec.reserve(num);
    for(std::size_t i=0; i < record_->field_count(); ++i) {
        if (indices.count(i) == 0) {
            vec.emplace_back(i);
        }
    }
    return from_keys(std::move(record), vec);
}

std::shared_ptr<meta::record_meta> shuffle_info::create_sort_key_meta(
    maybe_shared_ptr<meta::record_meta> record,
    std::vector<std::size_t> const& indices,
    std::vector<std::size_t> const& sort_key_indices
) {
    std::vector<std::size_t> merged{indices.begin(), indices.end()};
    merged.insert(merged.end(), sort_key_indices.begin(), sort_key_indices.end());
    return from_keys(std::move(record), merged);
}

std::vector<ordering> shuffle_info::create_sort_key_ordering(
    std::size_t group_key_count,
    std::vector<ordering> const& sort_key_ordering
) {
    std::vector<ordering> order(group_key_count, ordering::undefined);
    order.insert(order.end(), sort_key_ordering.begin(), sort_key_ordering.end());
    return order;
}

}
