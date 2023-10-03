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

#include <cstddef>
#include <optional>
#include <ostream>

namespace jogasaki::meta {

struct decimal_field_option {
    /**
     * @brief create default object, that has no precision/scale information
     */
    decimal_field_option() = default;

    /**
     * @brief create new object
     */
    decimal_field_option(
        std::optional<std::size_t> precision,
        std::optional<std::size_t> scale
    ) :
        precision_(precision),
        scale_(scale)
    {}

    std::optional<std::size_t> precision_{};  //NOLINT
    std::optional<std::size_t> scale_{};  //NOLINT
}; //NOLINT

bool operator==(decimal_field_option const& a, decimal_field_option const& b) noexcept;

std::ostream& operator<<(std::ostream& out, decimal_field_option const& value);

} // namespace

