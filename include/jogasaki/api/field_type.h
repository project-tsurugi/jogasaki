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

#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/decimal_field_option.h>
#include <jogasaki/api/time_of_day_field_option.h>
#include <jogasaki/api/time_point_field_option.h>

namespace jogasaki::api {

/**
 * @brief type information for a field
 */
class field_type {
public:

    /**
     * @brief construct empty object (kind undefined)
     */
    constexpr field_type() noexcept = default;

    /**
     * @brief destruct the object
     */
    virtual ~field_type() = default;

    field_type(field_type const& other) = default;
    field_type& operator=(field_type const& other) = default;
    field_type(field_type&& other) noexcept = default;
    field_type& operator=(field_type&& other) noexcept = default;

    /**
     * @brief getter for field type kind
     */
    [[nodiscard]] virtual field_type_kind kind() const noexcept = 0;

    /**
     * @return true if field type is valid
     * @return false otherwise
     */
    [[nodiscard]] explicit operator bool() const noexcept {
        return kind() != field_type_kind::undefined;
    }

    /**
     * @brief accessor to decimal field option
     * @return decimal type field option if the field type is decimal
     * @return nullptr otherwise
     */
    [[nodiscard]] virtual std::shared_ptr<decimal_field_option> const& decimal_option() const noexcept = 0;

    /**
     * @brief accessor to time_of_day field option
     * @return decimal type field option if the field type is time_of_day
     * @return nullptr otherwise
     */
    [[nodiscard]] virtual std::shared_ptr<time_of_day_field_option> const& time_of_day_option() const noexcept = 0;

    /**
     * @brief accessor to time_point field option
     * @return decimal type field option if the field type is time_point
     * @return nullptr otherwise
     */
    [[nodiscard]] virtual std::shared_ptr<time_point_field_option> const& time_point_option() const noexcept = 0;
};

} // namespace

