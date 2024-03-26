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
#include "handle_encode_errors.h"

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>

namespace jogasaki::utils {

void handle_encode_errors_impl(
    request_context& context,
    status res,
    std::string_view filepath,
    std::string_view position
) noexcept {
    if(res == status::ok) return;
    switch (res) {
        case status::err_data_corruption: {
            error::set_error_impl(
                context,
                error_code::data_corruption_exception,
                "Data inconsistency detected.",
                filepath,
                position,
                res,
                true // severe error
            );
            return;
        }
        case status::err_expression_evaluation_failure: {
            error::set_error_impl(
                context,
                error_code::value_evaluation_exception,
                "An error occurred in evaluating values. Encoding failed.",
                filepath,
                position,
                res,
                false
            );
            return;
        }
        case status::err_insufficient_field_storage: {
            error::set_error_impl(
                context,
                error_code::value_too_long_exception,
                "Insufficient storage to store field data.",
                filepath,
                position,
                res,
                false
            );
            return;
        }
        case status::err_invalid_runtime_value: {
            error::set_error_impl(
                context,
                error_code::invalid_runtime_value_exception,
                "detected invalid runtime value",
                filepath,
                position,
                res,
                false
            );
            return;
        }
        default:
            // no-op - only known error is handled
            break;
    }
}
}

