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
#include "record_meta.h"

#include <utility>
#include <boost/assert.hpp>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/impl/record_layout_creator.h>

namespace jogasaki::meta {

record_meta::record_meta(
    record_meta::fields_type fields,
    record_meta::nullability_type nullability,
    record_meta::value_offset_table_type value_offset_table,
    record_meta::nullity_offset_table_type nullity_offset_table,
    std::size_t record_alignment,
    std::size_t record_size
) :
    fields_(std::move(fields)),
    nullability_(std::move(nullability)),
    field_count_(fields_.size()),
    value_offset_table_(std::move(value_offset_table)),
    nullity_offset_table_(std::move(nullity_offset_table)),
    record_alignment_(record_alignment),
    record_size_(record_size)
{
    BOOST_ASSERT(field_count_ == nullability_.size()); // NOLINT
    BOOST_ASSERT(field_count_ == value_offset_table_.size()); // NOLINT
    BOOST_ASSERT(field_count_ == nullity_offset_table_.size()); // NOLINT
}

record_meta::record_meta(
    record_meta::fields_type fields,
    record_meta::nullability_type nullability,
    std::size_t record_size
) :
    fields_(std::move(fields)),
    nullability_(std::move(nullability)),
    field_count_(fields_.size())
{
    impl::record_layout_creator c{fields_, nullability_};
    value_offset_table_ = std::move(c.value_offset_table());
    nullity_offset_table_ = std::move(c.nullity_offset_table());
    record_alignment_ = c.record_alignment();
    BOOST_ASSERT(record_size == npos || c.record_size() <= record_size);  //NOLINT
    record_size_ = record_size != npos ? record_size : c.record_size();
}

field_type const& record_meta::operator[](record_meta::field_index_type index) const noexcept {
    return fields_[index];
}

field_type const& record_meta::at(record_meta::field_index_type index) const noexcept {
    return fields_[index];
}

record_meta::value_offset_type record_meta::value_offset(record_meta::field_index_type index) const noexcept {
    return value_offset_table_[index];
}

record_meta::nullity_offset_type record_meta::nullity_offset(record_meta::field_index_type index) const noexcept {
    return nullity_offset_table_[index];
}

bool record_meta::nullable(record_meta::field_index_type index) const noexcept {
    return nullability_[index];
}

std::size_t record_meta::record_alignment() const noexcept {
    return record_alignment_;
}

std::size_t record_meta::record_size() const noexcept {
    return record_size_;
}

std::size_t record_meta::field_count() const noexcept {
    return field_count_;
}

record_meta::field_iterator record_meta::begin() const noexcept {
    return fields_.begin();
}

record_meta::field_iterator record_meta::end() const noexcept {
    return fields_.end();
}

} // namespace

