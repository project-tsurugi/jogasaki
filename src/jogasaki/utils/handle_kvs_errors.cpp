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
#include "handle_kvs_errors.h"

namespace jogasaki::utils {

void handle_kvs_errors_impl(
    request_context& context,
    status res,
    std::string_view filepath,
    std::string_view position
) noexcept {
    if(res == status::ok) return;

    bool append_stacktrace = false;
    switch(res) {
        // warnings are context dependent and must be handled by the caller
        case status::already_exists: return;
        case status::not_found: return;
        case status::user_rollback: return;
        case status::waiting_for_other_transaction: return;

        case status::err_serialization_failure: {
            error::set_error_impl(
                context,
                error_code::cc_exception,
                "Serialization failed. ",
                filepath,
                position,
                res,
                append_stacktrace
            );
            return;
        }
        case status::err_conflict_on_write_preserve: {
            error::set_error_impl(
                context,
                error_code::conflict_on_write_preserve_exception,
                "Serialization failed due to conflict on write preserve. ",
                filepath,
                position,
                res,
                append_stacktrace
            );
            return;
        }
        case status::err_read_area_violation: {
            error::set_error_impl(
                context,
                error_code::read_operation_on_restricted_read_area_exception,
                "Read operation outside read area.",
                filepath,
                position,
                res,
                append_stacktrace
            );
            return;
        }
        case status::err_write_without_write_preserve: {
            error::set_error_impl(
                context,
                error_code::ltx_write_operation_without_write_preserve_exception,
                "Ltx write operation outside write preserve.",
                filepath,
                position,
                res,
                append_stacktrace
            );
            return;
        }
        case status::err_write_operation_by_rtx: {
            error::set_error_impl(
                context,
                error_code::write_operation_by_rtx_exception,
                "Write operation by rtx.",
                filepath,
                position,
                res,
                append_stacktrace
            );
            return;
        }
        case status::err_inactive_transaction: {
            error::set_error_impl(
                context,
                error_code::inactive_transaction_exception,
                "Current transaction is inactive (maybe aborted already.)",
                filepath,
                position,
                res,
                append_stacktrace
            );
            return;
        }
        case status::err_invalid_key_length: {
            error::set_error_impl(
                context,
                error_code::value_too_long_exception,
                "The key is too long to manipulate the kvs entry.",
                filepath,
                position,
                res,
                append_stacktrace
            );
            return;
        }
        default:
            // no-op - only known error is handled
            break;
    }
}
}

