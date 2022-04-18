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
#include <optional>
#include <type_traits>

#include <jogasaki/api/field_type.h>

namespace jogasaki::api {

/**
 * @brief record metadata holding information about field type and nullability
 */
class record_meta {
public:
    /// @brief field index type (origin = 0)
    using field_index_type = std::size_t;

    /**
     * @brief construct empty object
     */
    constexpr record_meta() = default;

    /**
     * @brief destruct the object
     */
    virtual ~record_meta() = default;

    record_meta(record_meta const& other) = delete;
    record_meta& operator=(record_meta const& other) = delete;
    record_meta(record_meta&& other) noexcept = delete;
    record_meta& operator=(record_meta&& other) noexcept = delete;

    /**
     * @brief getter for field type
     * @param index field index. Must be equal to, or greater than 0. Must be less than the field count.
     * @return field type
     * @warning if index is not in valid range, the behavior is undefined
     */
    [[nodiscard]] virtual field_type const& at(field_index_type index) const noexcept = 0;

    /**
     * @brief getter for the nullability for the field
     * @param index field index. Must be equal to, or greater than 0. Must be less than the field count.
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

    /**
     * @brief retrieve the field name
     * @param index field index. Must be equal to, or greater than 0. Must be less than the field count.
     * @return the field name
     */
    [[nodiscard]] virtual std::optional<std::string_view> field_name(field_index_type index) const noexcept = 0;
};

} // namespace

