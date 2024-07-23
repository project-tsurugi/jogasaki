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

struct octet_field_option {
    /**
     * @brief create default object, that has no information
     */
    octet_field_option() = default;

    /**
     * @brief create new object
     */
    octet_field_option(
        bool varying,
        std::optional<std::size_t> length
    ) :
        varying_(varying),
        length_(length)
    {}

    bool varying_{true};  //NOLINT
    std::optional<std::size_t> length_{};  //NOLINT
}; //NOLINT

bool operator==(octet_field_option const& a, octet_field_option const& b) noexcept;

std::ostream& operator<<(std::ostream& out, octet_field_option const& value);

}  // namespace jogasaki::meta
