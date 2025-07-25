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
#pragma once

#include <memory>
#include <variant>

#include <jogasaki/api/character_field_option.h>
#include <jogasaki/api/decimal_field_option.h>
#include <jogasaki/api/field_type.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/time_of_day_field_option.h>
#include <jogasaki/api/time_point_field_option.h>
#include <jogasaki/meta/field_type.h>

namespace jogasaki::api::impl {

/**
 * @brief type information for a field
 */
class field_type : public api::field_type {
public:

    using option_type = std::variant<
        std::monostate,
        std::shared_ptr<character_field_option>,
        std::shared_ptr<octet_field_option>,
        std::shared_ptr<decimal_field_option>,
        std::shared_ptr<time_of_day_field_option>,
        std::shared_ptr<time_point_field_option>
    >;

    /**
     * @brief construct empty object (kind undefined)
     */
    constexpr field_type() noexcept = default;

    /**
     * @brief construct new object
     */
    explicit field_type(meta::field_type type) noexcept;

    /**
     * @brief getter for type kind
     */
    [[nodiscard]] api::field_type_kind kind() const noexcept override;;

    /**
     * @brief accessor to character field option
     * @return character type field option if the field type is character
     * @return nullptr otherwise
     */
    [[nodiscard]] std::shared_ptr<character_field_option> const& character_option() const noexcept override;

    /**
     * @brief accessor to octet field option
     * @return octet type field option if the field type is octet
     * @return nullptr otherwise
     */
    [[nodiscard]] std::shared_ptr<octet_field_option> const& octet_option() const noexcept override;

    /**
     * @brief accessor to decimal field option
     * @return decimal type field option if the field type is decimal
     * @return nullptr otherwise
     */
    [[nodiscard]] std::shared_ptr<decimal_field_option> const& decimal_option() const noexcept override;

    /**
     * @brief accessor to time_of_day field option
     * @return decimal type field option if the field type is time_of_day
     * @return nullptr otherwise
     */
    [[nodiscard]] std::shared_ptr<time_of_day_field_option> const& time_of_day_option() const noexcept override;

    /**
     * @brief accessor to time_point field option
     * @return decimal type field option if the field type is time_point
     * @return nullptr otherwise
     */
    [[nodiscard]] std::shared_ptr<time_point_field_option> const& time_point_option() const noexcept override;

private:
    meta::field_type type_{};
    option_type option_{};
};

} // namespace

