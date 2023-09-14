/*
 * Copyright 2018-2023 tsurugi project.
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

namespace jogasaki::api::kvsservice {

/**
 * @brief represents status code of API operations.
 */
enum class status : std::int32_t {

    /**
     * @brief operation was succeeded.
     */
    ok = 0,

    /**
     * @brief the target element does not exist.
     */
    not_found = 1,

    /**
     * @brief the target element already exists.
     */
    already_exists = 2,

    /**
     * @brief transaction operation is rollbacked by user.
     */
    user_rollback = 3,

    /**
     * @brief the operation is waiting for other transaction
     */
    waiting_for_other_transaction = 4,

    /**
     * @brief unknown errors.
     */
    err_unknown = 100,

    /**
     * @brief I/O error.
     */
    err_io_error = 102,

    /**
     * @brief API arguments are invalid.
     */
    err_invalid_argument = 103,

    /**
     * @brief API state is invalid.
     */
    err_invalid_state = 104,

    /**
     * @brief operation is unsupported.
     */
    err_unsupported = 105,

    /**
     * @brief transaction operation met an user-defined error.
     * @details this code is returned only from transaction_exec() and transaction_commit()
     */
    err_user_error = 106,

    /**
     * @brief transaction is aborted
     */
    err_aborted = 107,

    /**
     * @brief transaction is aborted, but retry might resolve the situation
     */
    err_aborted_retryable = 108,

    /**
     * @brief api call timed out
     */
    err_time_out = 109,

    /**
     * @brief the feature is not yet implemented
     */
    err_not_implemented = 110,

    /**
     * @brief the operation is not valid
     */
    err_illegal_operation = 111,

    /**
     * @brief the operation conflicted on write preserve
     */
    err_conflict_on_write_preserve = 112,

    // NOTE: sharksfin::StatusCode doesn't have the error code of -13

    /**
     * @brief long tx issued write operation without preservation
     */
    err_write_without_write_preserve = 114,

    /**
     * @brief transaction is inactive
     * @details transaction is inactive since it's already committed or aborted. The request is failed.
     */
    err_inactive_transaction = 115,

    /**
     * @brief requested operation is blocked by concurrent operation
     * @details the request cannot be fulfilled due to the operation concurrently executed by other transaction.
     * After the blocking transaction completes, re-trying the request may lead to different result.
     */
    err_blocked_by_concurrent_operation = 116,

    /**
     * @brief reached resource limit and request could not be accomplished
     */
    err_resource_limit_reached = 117,

    /**
     * @brief key length passed to the API is invalid
     */
    err_invalid_key_length = 118,

    /**
     * @brief The operation result data is too large
     */
    err_result_too_large = 1'001,

    /**
     * @brief Target resource is not authorized.
     */
    err_not_authorized = 2'001,

    /**
     * @brief Transaction is aborted by writing out of write preservation, or writing in read only transaction.
     */
    err_write_protected = 12'002,

    /**
     * @brief The specified table is not found.
     */
    err_table_not_found = 20'001,

    /**
     * @brief The specified column is not found.
     */
    err_column_not_found = 20'002,

    /**
     * @brief The column type is inconsistent.
     */
    err_column_type_mismatch = 20'003,

    /**
     * @brief The search key is mismatch for the table or index.
     */
    err_mismatch_key = 20'011,

    /**
     * @brief Several columns are not specified in {@code PUT} operation.
     */
    err_incomplete_columns = 20'021,

    /**
     * @brief Operations was failed by integrity constraint violation.
     */
    err_integrity_constraint_violation = 30'001,

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
        case status::waiting_for_other_transaction: return "waiting_for_other_transaction"sv;

        case status::err_unknown: return "err_unknown"sv;
        case status::err_io_error: return "err_io_error"sv;
        case status::err_invalid_argument: return "err_invalid_argument"sv;
        case status::err_invalid_state: return "err_invalid_state"sv;
        case status::err_unsupported: return "err_unsupported"sv;
        case status::err_user_error: return "err_user_error"sv;
        case status::err_aborted: return "err_aborted"sv;
        case status::err_aborted_retryable: return "err_aborted_retryable"sv;
        case status::err_time_out: return "err_time_out"sv;
        case status::err_not_implemented: return "err_not_implemented"sv;
        case status::err_illegal_operation: return "err_illegal_operation"sv;
        case status::err_conflict_on_write_preserve: return "err_conflict_on_write_preserve"sv;
        case status::err_write_without_write_preserve: return "err_write_without_write_preserve"sv;
        case status::err_inactive_transaction: return "err_inactive_transaction"sv;
        case status::err_blocked_by_concurrent_operation: return "err_blocked_by_concurrent_operation"sv;
        case status::err_resource_limit_reached: return "err_resource_limit_reached"sv;
        case status::err_invalid_key_length: return "err_invalid_key_length"sv;
        case status::err_result_too_large: return "err_result_too_large"sv;
        case status::err_not_authorized: return "err_not_authorized"sv;
        case status::err_transaction_inactive: return "err_transaction_inactive"sv;
        case status::err_write_protected: return "err_write_protected"sv;
        case status::err_table_not_found: return "err_table_not_found"sv;
        case status::err_column_not_found: return "err_column_not_found"sv;
        case status::err_column_type_mismatch: return "err_column_type_mismatch"sv;
        case status::err_mismatch_key: return "err_mismatch_key"sv;
        case status::err_incomplete_columns: return "err_incomplete_columns"sv;
        case status::err_integrity_constraint_violation: return "err_integrity_constraint_violation"sv;
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

}
