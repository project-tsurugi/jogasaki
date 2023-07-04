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
enum class status : std::int64_t {

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
    err_unknown = -1,

    /**
     * @brief I/O error.
     */
    err_io_error = -2,

    /**
     * @brief API arguments are invalid.
     */
    err_invalid_argument = -3,

    /**
     * @brief API state is invalid.
     */
    err_invalid_state = -4,

    /**
     * @brief operation is unsupported.
     */
    err_unsupported = -5,

    /**
     * @brief transaction operation met an user-defined error.
     * @details this code is returned only from transaction_exec() and transaction_commit()
     */
    err_user_error = -6,

    /**
     * @brief transaction is aborted
     */
    err_aborted = -7,

    /**
     * @brief transaction is aborted, but retry might resolve the situation
     */
    err_aborted_retryable = -8,

    /**
     * @brief api call timed out
     */
    err_time_out = -9,

    /**
     * @brief the feature is not yet implemented
     */
    err_not_implemented = -10,

    /**
     * @brief the operation is not valid
     */
    err_illegal_operation = -11,

    /**
     * @brief the operation conflicted on write preserve
     */
    err_conflict_on_write_preserve = -12,

    /**
     * @brief long tx issued write operation without preservation
     */
    err_write_without_write_preserve = -14,

    /**
     * @brief transaction is inactive
     * @details transaction is inactive since it's already committed or aborted. The request is failed.
     */
    err_inactive_transaction = -15,

    /**
     * @brief requested operation is blocked by concurrent operation
     * @details the request cannot be fulfilled due to the operation concurrently executed by other transaction.
     * After the blocking transaction completes, re-trying the request may lead to different result.
     */
    err_blocked_by_concurrent_operation = -16,

    /**
     * @brief reached resource limit and request could not be accomplished
     */
    err_resource_limit_reached = -17,

    /**
     * @brief key length passed to the API is invalid
     */
    err_invalid_key_length = -18,

    /**
     * @brief The operation result data is too large
     */
    err_result_too_large = -1'001,

    /**
     * @brief Target resource is not authorized.
     */
    err_not_authorized = -2'001,

    /**
     * @brief Transaction is not active.
     */
    err_transaction_inactive = -10'001,

    /**
     * @brief Transaction is aborted by writing out of write preservation, or writing in read only transaction.
     */
    err_write_protected = -12'002,

    /**
     * @brief The specified table is not found.
     */
    err_table_not_found = -20'001,

    /**
     * @brief The specified column is not found.
     */
    err_column_not_found = -20'002,

    /**
     * @brief The column type is inconsistent.
     */
    err_column_type_mismatch = -20'003,

    /**
     * @brief The search key is mismatch for the table or index.
     */
    err_mismatch_key = -20'011,

    /**
     * @brief Several columns are not specified in {@code PUT} operation.
     */
    err_incomplete_columns = -20'021,

    /**
     * @brief Operations was failed by integrity constraint violation.
     */
    err_integrity_constraint_violation = -30'001,

};
}
