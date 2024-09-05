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
#include "assignment.h"

#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <glog/logging.h>

#include <takatori/type/data.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/data/any.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/expr/details/cast_evaluation.h>
#include <jogasaki/executor/expr/error.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::conv {

using namespace jogasaki::executor::process::impl;

using takatori::util::string_builder;

status conduct_assignment_conversion(
    takatori::type::data const& source_type,
    takatori::type::data const& target_type,
    data::any const& in,
    data::any& out,
    request_context& ctx,
    memory::lifo_paged_memory_resource* resource
) {
    expr::evaluator_context ectx{resource, nullptr}; // evaluate no function
    ectx.set_loss_precision_policy(expr::loss_precision_policy::implicit);
    auto converted = expr::details::conduct_cast(ectx, source_type, target_type, in);
    if(! converted.error()) {
        out = converted;
        return status::ok;
    }
    auto err = converted.to<expr::error>();
    if(err.kind() == expr::error_kind::unsupported) {
        auto res = status::err_unsupported;
        auto [msg, value_msg] = expr::create_conversion_error_message(ectx);
        set_error(
            ctx,
            error_code::unsupported_runtime_feature_exception,
            msg,
            res
        );
        return res;
    }
    if(err.kind() == expr::error_kind::lost_precision_value_too_long) {
        auto res = status::err_expression_evaluation_failure;
        auto [msg, value_msg] = expr::create_conversion_error_message(ectx);
        set_error(
            ctx,
            error_code::value_too_long_exception,
            msg,
            res
        );
        if(global::config_pool()->log_msg_user_data()) {
            VLOG(log_error) << msg << " " << value_msg;
        }
        return res;
    }
    auto res = status::err_expression_evaluation_failure;
    auto [msg, value_msg] = expr::create_conversion_error_message(ectx);
    auto m = string_builder{} << "error in evaluating expression: " << msg << string_builder::to_string;
    set_error(
        ctx,
        error_code::value_evaluation_exception,
        m,
        res
    );
    if(global::config_pool()->log_msg_user_data()) {
        VLOG(log_error) << m << " " << value_msg;
    }
    return res;
}

status conduct_unifying_conversion(
    takatori::type::data const& source_type,
    takatori::type::data const& target_type,
    data::any const& in,
    data::any& out,
    memory::lifo_paged_memory_resource* resource
) {
    expr::evaluator_context ectx{resource, nullptr}; // evaluate no function
    // unifying conversion doesn't lose precision
    ectx.set_loss_precision_policy(expr::loss_precision_policy::ignore);
    auto converted = expr::details::conduct_cast(ectx, source_type, target_type, in);
    if(! converted.error()) {
        out = converted;
        return status::ok;
    }
    return status::err_expression_evaluation_failure;
}

bool to_require_conversion(
    takatori::type::data const& source_type,
    takatori::type::data const& target_type
) {
    return source_type != target_type;
}

}  // namespace jogasaki::executor::conv
