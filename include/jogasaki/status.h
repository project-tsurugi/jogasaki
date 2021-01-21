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

namespace jogasaki {

/**
 * @brief status code
 */
enum class status : std::size_t {
    ok = 0,
    not_found,
    already_exists,
    user_rollback,
    err_io_error,
    err_parse_error,
    err_translator_error,
    err_compiler_error,
    err_invalid_argument,
    err_invalid_state,
    err_unsupported,
    err_user_error,
    err_aborted,
    err_aborted_retryable,
    err_unknown,
    undefined,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(status value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case status::ok: return "ok"sv;
        case status::not_found: return "not_found"sv;
        case status::already_exists: return "already_exists"sv;
        case status::user_rollback: return "user_rollback"sv;
        case status::err_io_error: return "err_io_error"sv;
        case status::err_parse_error: return "err_parse_error"sv;
        case status::err_translator_error: return "err_translator_error"sv;
        case status::err_compiler_error: return "err_compiler_error"sv;
        case status::err_invalid_argument: return "err_invalid_argument"sv;
        case status::err_invalid_state: return "err_invalid_state"sv;
        case status::err_unsupported: return "err_unsupported"sv;
        case status::err_user_error: return "err_user_error"sv;
        case status::err_aborted: return "err_aborted"sv;
        case status::err_aborted_retryable: return "err_aborted_retryable"sv;
        case status::err_unknown: return "err_unknown"sv;
        case status::undefined: return "undefined"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, status value) {
    return out << to_string_view(value);
}

/// @brief a set of expression_kind.
using status_code_set = takatori::util::enum_set<
        status,
        status::ok,
        status::undefined>;

} // namespace

