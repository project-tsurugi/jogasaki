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
#include <takatori/util/enum_set.h>

namespace tateyama::api::endpoint {

/**
 * @brief response code
 */
enum class response_code : std::int64_t {
    /// @brief error occurred but the cause is unknown
    unknown = -1,

    /// @brief successful completion
    success = 0,

    /// @brief temporary response notifying the request processing started
    started = 1,

    /// @brief error occurred in the application domain
    application_error = 2,

};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(response_code value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case response_code::unknown: return "unknown"sv;
        case response_code::success: return "success"sv;
        case response_code::started: return "started"sv;
        case response_code::application_error: return "application_error"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, response_code value) {
    return out << to_string_view(value);
}

} // namespace

