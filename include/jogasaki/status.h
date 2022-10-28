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
enum class status : std::int64_t {
    ok = 0,

    // warnings
    not_found = 1,
    already_exists = 2,
    user_rollback = 3,

    // errors
    err_unknown = -1,
    err_io_error = -2,
    err_parse_error = -3,
    err_translator_error = -4,
    err_compiler_error = -5,
    err_invalid_argument = -6,
    err_invalid_state = -7,
    err_unsupported = -8,
    err_user_error = -9,
    err_aborted = -10,
    err_aborted_retryable = -11,
    err_not_found = -12,
    err_already_exists = -13,
    err_inconsistent_index = -14,
    err_time_out = -15,
    err_integrity_constraint_violation = -16,
    err_expression_evaluation_failure = -17,
    err_unresolved_host_variable = -18,
    err_type_mismatch = -19,
    err_not_implemented = -20,
    err_illegal_operation = -21,
    err_missing_operation_target = -22,
    err_conflict_on_write_preserve = -23,
    err_inactive_transaction = -24,
    err_waiting_for_other_transaction = -25,
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

        case status::err_unknown: return "err_unknown"sv;
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
        case status::err_not_found: return "err_not_found"sv;
        case status::err_already_exists: return "err_already_exists"sv;
        case status::err_inconsistent_index: return "err_inconsistent_index"sv;
        case status::err_time_out: return "err_time_out"sv;
        case status::err_integrity_constraint_violation: return "err_integrity_constraint_violation"sv;
        case status::err_expression_evaluation_failure: return "err_expression_evaluation_failure"sv;
        case status::err_unresolved_host_variable: return "err_unresolved_host_variable"sv;
        case status::err_type_mismatch: return "err_type_mismatch"sv;
        case status::err_not_implemented: return "err_not_implemented"sv;
        case status::err_illegal_operation: return "err_illegal_operation"sv;
        case status::err_missing_operation_target: return "err_missing_operation_target"sv;
        case status::err_conflict_on_write_preserve: return "err_conflict_on_write_preserve"sv;
        case status::err_inactive_transaction: return "err_inactive_transaction"sv;
        case status::err_waiting_for_other_transaction: return "err_waiting_for_other_transaction"sv;
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

} // namespace

