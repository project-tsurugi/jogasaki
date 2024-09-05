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

#include <takatori/util/string_builder.h>

#include <jogasaki/data/any.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/status.h>

#include "../operation_status.h"

namespace jogasaki::executor::process::impl::ops::details {

using takatori::util::string_builder;

//NOLINTBEGIN
#define handle_expression_error(ctx, a, ectx)                                                                          \
    ::jogasaki::executor::process::impl::ops::details::handle_expression_error_impl(                                   \
        ctx,                                                                                                           \
        a,                                                                                                             \
        ectx,                                                                                                          \
        __FILE__,                                                                                                      \
        line_number_string                                                                                             \
    )
//NOLINTEND

template<class Context>
operation_status handle_expression_error_impl(
    Context& ctx,
    data::any res,
    jogasaki::executor::expr::evaluator_context const& ectx,
    std::string_view filepath,
    std::string_view position) {
    auto err = res.to<expr::error>();
    if (err.kind() == expr::error_kind::unsupported) {
        auto rc = status::err_unsupported;
        error::set_error_impl(
            *ctx.req_context(),
            error_code::unsupported_runtime_feature_exception,
            string_builder{} << "unsupported expression is used" << string_builder::to_string,
            filepath,
            position,
            rc,
            false);
        ctx.abort();
        return {operation_status_kind::aborted};
    }
    if (err.kind() == expr::error_kind::lost_precision_value_too_long) {
        auto rc = status::err_expression_evaluation_failure;
        error::set_error_impl(
            *ctx.req_context(),
            error_code::value_too_long_exception,
            "evaluated value was too long",
            filepath,
            position,
            rc,
            false);
        ctx.abort();
        return {operation_status_kind::aborted};
    }
    auto rc = status::err_expression_evaluation_failure;

    std::stringstream ss{};
    ss << "an error (" << res.to<expr::error>() << ") occurred:[";
    bool first = true;
    for(auto&& e : ectx.errors()) {
        if (! first) {
            ss << ", ";
        }
        ss << e;
        first = false;
    }
    ss << "]";
    error::set_error_impl(
        *ctx.req_context(),
        error_code::value_evaluation_exception,
        ss.str(),
        filepath,
        position,
        rc,
        false);
    ctx.abort();
    return {operation_status_kind::aborted};
}

}  // namespace jogasaki::executor::process::impl::ops::details
