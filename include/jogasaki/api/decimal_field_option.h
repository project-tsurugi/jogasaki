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

#include <optional>

namespace jogasaki::api {

/**
 * @brief type information for a decimal field
 */
class decimal_field_option {
public:

    /**
     * @brief construct empty object
     */
    constexpr decimal_field_option() noexcept = default;

    /**
     * @brief construct new object
     */
    constexpr decimal_field_option(
        std::optional<std::size_t> precision,
        std::optional<std::size_t> scale
    ) noexcept :
        precision_(precision),
        scale_(scale)
    {}
    /**
     * @brief destruct the object
     */
    ~decimal_field_option() = default;

    decimal_field_option(decimal_field_option const& other) = default;
    decimal_field_option& operator=(decimal_field_option const& other) = default;
    decimal_field_option(decimal_field_option&& other) noexcept = default;
    decimal_field_option& operator=(decimal_field_option&& other) noexcept = default;

    [[nodiscard]] std::optional<std::size_t> precision() const noexcept {
        return precision_;
    }

    [[nodiscard]] std::optional<std::size_t> scale() const noexcept {
        return scale_;
    }

private:
    std::optional<std::size_t> precision_{};  //NOLINT
    std::optional<std::size_t> scale_{};  //NOLINT

};

} // namespace

