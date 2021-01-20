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
#pragma once

#include <cstddef>
#include <algorithm>
#include <type_traits>

#include <jogasaki/api/field_type.h>

namespace jogasaki::api {

/**
 * @brief record metadata holding information about field types, nullability and binary encoding of records.
 * @details based on the record metadata and knowledge on binary encoding/bits layout, record_meta provides information
 * to access its data via record_ref accessor (e.g. offset for field value or nullity bit.)
 */
class record_meta {
public:
    /// @brief fields type
    using fields_type = std::vector<field_type>;

    /// @brief iterator for fields
    using field_iterator = fields_type::const_iterator;

    /// @brief field index type (origin = 0)
    using field_index_type = std::size_t;

    /// @brief the value indicating invalid offset
    constexpr static std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief construct empty object
     */
    constexpr record_meta() = default;

    /**
     * @brief getter for field type - same as operator[] but friendly style for pointers
     * @param index field index
     * @return field type
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] virtual field_type const& at(field_index_type index) const noexcept = 0;

    /**
     * @brief getter for the nullability for the field
     * @param index field index
     * @return true if the field is nullable
     * @return false otherwise
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] virtual bool nullable(field_index_type index) const noexcept = 0;

    /**
     * @brief retrieve number of fields in the record
     * @return number of the fields
     */
    [[nodiscard]] virtual std::size_t field_count() const noexcept = 0;

};

} // namespace

