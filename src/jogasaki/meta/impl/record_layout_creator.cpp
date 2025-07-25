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
#include "record_layout_creator.h"

#include <algorithm>
#include <boost/assert.hpp>

#include <jogasaki/constants.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/round.h>

namespace jogasaki::meta::impl {

record_layout_creator::record_layout_creator(
    record_layout_creator::fields_type const& fields,
    record_layout_creator::nullability_type const& nullability
) {
    std::size_t nullity_offset = 0;
    auto field_count = fields.size();
    BOOST_ASSERT(field_count == nullability.size()); // NOLINT
    for(std::size_t i = 0; i < field_count; ++i) {
        std::size_t pos = record_meta::npos;
        if (nullability[i]) {
            pos = nullity_offset;
            ++nullity_offset;
        }
        nullity_offset_table_.emplace_back(pos);
    }
    auto nullity_bytes = utils::round_up_to_power_of_two(
        (nullability.count() + bits_per_byte - 1) / bits_per_byte
    );
    std::size_t record_max_align = 1UL;
    std::size_t cur = nullity_bytes;
    for(std::size_t i = 0; i < field_count; ++i) {
        auto&& field = fields[i];
        auto alignment = field.runtime_type_alignment();
        record_max_align = std::max(record_max_align, alignment);
        cur = (cur + alignment - 1) / alignment * alignment;
        value_offset_table_.emplace_back(cur);
        cur += field.runtime_type_size();
    }
    record_alignment_ = record_max_align;
    record_size_ = (cur + record_alignment_ - 1) / record_alignment_ * record_alignment_;
    BOOST_ASSERT(record_max_align <= record_meta::max_alignment); //NOLINT
    BOOST_ASSERT(record_meta::max_alignment % record_max_align == 0); //NOLINT
}

record_layout_creator::value_offset_table_type& record_layout_creator::value_offset_table() noexcept {
    return value_offset_table_;
}

record_layout_creator::nullity_offset_table_type& record_layout_creator::nullity_offset_table() noexcept {
    return nullity_offset_table_;
}

size_t record_layout_creator::record_alignment() const noexcept {
    return record_alignment_;
}

size_t record_layout_creator::record_size() const noexcept {
    return record_size_;
}
} // namespace

