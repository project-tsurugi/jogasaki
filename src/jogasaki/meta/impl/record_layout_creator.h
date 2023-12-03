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
#pragma once

#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/round.h>

namespace jogasaki::meta::impl {

/**
 * @brief Record data buffer layout creator
 * @details Records are binary encoded as follows:
 * First, nullity bits field is placed at the beginning. The bits fields alignment is 1 byte and
 * its size is the ceiling of number of fields divided by 8(bits_per_byte). Then, the field values encoded
 * by the native format of its runtime type are ordered respecting alignment of each runtime type.
 * (The field order has been given to record_meta via constructor.)
 */
class record_layout_creator {
public:
    using fields_type = record_meta::fields_type;
    using nullability_type = record_meta::nullability_type;
    using value_offset_table_type = record_meta::value_offset_table_type;
    using nullity_offset_table_type = record_meta::nullity_offset_table_type;

    record_layout_creator(fields_type const& fields, nullability_type const& nullability);

    [[nodiscard]] value_offset_table_type& value_offset_table() noexcept;

    [[nodiscard]] nullity_offset_table_type& nullity_offset_table() noexcept;

    [[nodiscard]] size_t record_alignment() const noexcept;

    [[nodiscard]] size_t record_size() const noexcept;

private:
    value_offset_table_type value_offset_table_{};
    nullity_offset_table_type nullity_offset_table_{};
    std::size_t record_alignment_{};
    std::size_t record_size_{};
};

}  // namespace jogasaki::meta::impl
