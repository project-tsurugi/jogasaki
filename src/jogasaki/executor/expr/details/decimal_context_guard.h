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

#include <cstdint>
#include <decimal.hh>

namespace jogasaki::executor::expr::details {

/**
 * @brief decimal context guard
 * @details This class is used to guard the decimal context on decimal::context (thread local).
 * The setters remember the original values in decimal::context and restore them on destruction.
*/
class decimal_context_guard {
public:

    /**
     * @brief create object
    */
    decimal_context_guard() = default;

    decimal_context_guard(decimal_context_guard const& other) = delete;
    decimal_context_guard& operator=(decimal_context_guard const& other) = delete;
    decimal_context_guard(decimal_context_guard&& other) noexcept = delete;
    decimal_context_guard& operator=(decimal_context_guard&& other) noexcept = delete;

    /**
     * @brief destruct object and restore the original values
    */
    ~decimal_context_guard() noexcept;

    /**
     * @brief set round mode on decimal::context, and remember the original value
    */
    decimal_context_guard& round(std::int32_t round);

private:
    bool round_set_{};
    std::int32_t round_{};
};

}  // namespace jogasaki::executor::expr::details
