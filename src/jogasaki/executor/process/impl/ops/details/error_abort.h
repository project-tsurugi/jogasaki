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

template <class T>
operation_status error_abort(T& ctx, status res) {
    ctx.abort();
    switch(res) {
        case status::err_unique_constraint_violation:
            set_error(
                *ctx.req_context(),
                error_code::unique_constraint_violation_exception,
                string_builder{} <<
                    "Unique constraint violation occurred." << string_builder::to_string,
                res
            );
            break;
        case status::err_integrity_constraint_violation:
            set_error(
                *ctx.req_context(),
                error_code::not_null_constraint_violation_exception,
                string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
                res
            );
            break;
        case status::err_expression_evaluation_failure:
            set_error(
                *ctx.req_context(),
                error_code::value_evaluation_exception,
                string_builder{} << "An error occurred in evaluating values." << string_builder::to_string,
                res
            );
            break;
        case status::err_serialization_failure:
            set_error(
                *ctx.req_context(),
                error_code::cc_exception,
                string_builder{} << "Serialization failed." << string_builder::to_string,
                res
            );
            break;
        case status::err_conflict_on_write_preserve:
            set_error(
                *ctx.req_context(),
                error_code::conflict_on_write_preserve_exception,
                string_builder{} << "Occ read conflicted on some write preserve and aborted." << string_builder::to_string,
                res
            );
            break;
        case status::err_inactive_transaction:
            set_error(
                *ctx.req_context(),
                error_code::inactive_transaction_exception,
                string_builder{} << "Current transaction is inactive (maybe aborted already.)" << string_builder::to_string,
                res
            );
            break;
        case status::err_data_corruption:
            set_error(
                *ctx.req_context(),
                error_code::data_corruption_exception,
                string_builder{} << "Data inconsistency detected." << string_builder::to_string,
                res
            );
            break;
        case status::err_unsupported:
            set_error(
                *ctx.req_context(),
                error_code::unsupported_runtime_feature_exception,
                string_builder{} << "Executed an unsupported feature." << string_builder::to_string,
                res
            );
            break;
        default:
            set_error(
                *ctx.req_context(),
                error_code::sql_execution_exception,
                string_builder{} << "creating transaction failed with error:" << res << string_builder::to_string,
                res
            );
            break;
    }
    return {operation_status_kind::aborted};
}

}



