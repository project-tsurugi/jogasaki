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

#include <takatori/util/string_builder.h>

#include <jogasaki/status.h>
#include <jogasaki/error_code.h>
#include <jogasaki/error/error_info_factory.h>
#include "../operation_status.h"

namespace jogasaki::executor::process::impl::ops::details {

using takatori::util::string_builder;

#define error_abort(ctx, res) ::jogasaki::executor::process::impl::ops::details::error_abort_impl(ctx, res, __FILE__, line_number_string) //NOLINT

template <class T>
operation_status error_abort_impl(
    T& ctx,
    status res,
    std::string_view filepath,
    std::string_view position
) {
    ctx.abort();
    if(ctx.req_context()->error_info()) {
        return {operation_status_kind::aborted};
    }
    switch(res) {
        case status::err_unique_constraint_violation:
            error::set_error_impl(
                *ctx.req_context(),
                error_code::unique_constraint_violation_exception,
                string_builder{} <<
                    "Unique constraint violation occurred." << string_builder::to_string,
                filepath,
                position,
                res,
                false
            );
            break;
        case status::err_integrity_constraint_violation:
            error::set_error_impl(
                *ctx.req_context(),
                error_code::not_null_constraint_violation_exception,
                string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
                filepath,
                position,
                res,
                false
            );
            break;
        case status::err_expression_evaluation_failure:
            error::set_error_impl(
                *ctx.req_context(),
                error_code::value_evaluation_exception,
                string_builder{} << "An error occurred in evaluating values." << string_builder::to_string,
                filepath,
                position,
                res,
                false
            );
            break;
        case status::err_data_corruption:
            error::set_error_impl(
                *ctx.req_context(),
                error_code::data_corruption_exception,
                string_builder{} << "Data inconsistency detected." << string_builder::to_string,
                filepath,
                position,
                res,
                true // this is high severity
            );
            break;
        case status::err_unsupported:
            error::set_error_impl(
                *ctx.req_context(),
                error_code::unsupported_runtime_feature_exception,
                string_builder{} << "Executed an unsupported feature." << string_builder::to_string,
                filepath,
                position,
                res,
                true
            );
            break;
        case status::err_insufficient_field_storage:
            error::set_error_impl(
                *ctx.req_context(),
                error_code::sql_limit_reached_exception,
                "Insufficient storage to store field data.",
                filepath,
                position,
                res,
                false
            );
            break;
        default:
            error::set_error_impl(
                *ctx.req_context(),
                error_code::sql_execution_exception,
                string_builder{} << "Unexpected error occurred. status:" << res << string_builder::to_string,
                filepath,
                position,
                res,
                true
            );
            break;
    }
    return {operation_status_kind::aborted};
}

}



