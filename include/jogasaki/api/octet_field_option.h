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

#include <optional>

namespace jogasaki::api {

/**
 * @brief type information for a octet field
 */
class octet_field_option {
public:

    /**
     * @brief construct empty object
     */
    constexpr octet_field_option() noexcept = default;

    /**
     * @brief construct new object
     */
    constexpr octet_field_option(
        bool varying,
        std::optional<std::size_t> length
    ) noexcept :
        varying_(varying),
        length_(length)
    {}
    /**
     * @brief destruct the object
     */
    ~octet_field_option() = default;

    octet_field_option(octet_field_option const& other) = default;
    octet_field_option& operator=(octet_field_option const& other) = default;
    octet_field_option(octet_field_option&& other) noexcept = default;
    octet_field_option& operator=(octet_field_option&& other) noexcept = default;

    [[nodiscard]] std::optional<std::size_t> length() const noexcept {
        return length_;
    }

    [[nodiscard]] bool varying() const noexcept {
        return varying_;
    }
private:
    bool varying_{true};
    std::optional<std::size_t> length_{};  //NOLINT

};

}  // namespace jogasaki::api
