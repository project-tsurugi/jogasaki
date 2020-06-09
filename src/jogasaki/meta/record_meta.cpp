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
#include "record_meta.h"

namespace jogasaki::meta {

record_meta::record_meta(record_meta::value_entity_type entity, record_meta::nullability_entity_type nullability,
    record_meta::value_offset_table_type value_offset_table,
    record_meta::nullity_offset_table_type nullity_offset_table, std::size_t record_alignment, std::size_t record_size)
    :
    entity_(std::move(entity)), nullability_(std::move(nullability)), field_count_(entity_.size()),
    value_offset_table_(std::move(value_offset_table)), nullity_offset_table_(std::move(nullity_offset_table)),
    record_alignment_(record_alignment), record_size_(record_size) {
    assert(field_count_ == nullability_.size()); // NOLINT
    assert(field_count_ == value_offset_table_.size()); // NOLINT
    assert(field_count_ == nullity_offset_table_.size()); // NOLINT
}

record_meta::record_meta(record_meta::value_entity_type entity, record_meta::nullability_entity_type nullability) :
    entity_(std::move(entity)), nullability_(std::move(nullability)), field_count_(entity_.size()) {
    assert(field_count_ == nullability_.size()); // NOLINT
    calculate_default_layout_offset();
}

field_type const &record_meta::operator[](record_meta::field_index_type index) const noexcept {
    return entity_[index];
}

field_type const &record_meta::at(record_meta::field_index_type index) const noexcept {
    return entity_[index];
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

void record_meta::calculate_default_layout_offset() {
    std::size_t cur = 0;
    std::size_t record_max_align = 1;
    for(std::size_t i = 0; i < field_count_; ++i) {
        auto&& field = entity_[i];
        auto alignment = field.runtime_type_alignment();
        record_max_align = std::max(record_max_align, alignment);
        cur = (cur + alignment - 1) / alignment * alignment;
        value_offset_table_.emplace_back(cur);
        cur += field.runtime_type_size();
    }
    std::size_t nullity_offset = cur * bits_per_byte;
    for(std::size_t i = 0; i < field_count_; ++i) {
        std::size_t pos = npos;
        if (nullability_[i]) {
            pos = nullity_offset;
            ++nullity_offset;
        }
        nullity_offset_table_.emplace_back(pos);
    }
    cur += (nullability_.count() + bits_per_byte - 1) / bits_per_byte;
    record_alignment_ = record_max_align;
    record_size_ = (cur + record_alignment_ - 1) / record_alignment_ * record_alignment_;
    assert(record_max_align <= max_alignment); //NOLINT
    assert(max_alignment % record_max_align == 0); //NOLINT
}
} // namespace

