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
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/expression/details/cast_evaluation.h>
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
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
    expression::evaluator_context ectx{resource};
    ectx.set_loss_precision_policy(expression::loss_precision_policy::implicit);
    auto converted = expression::details::conduct_cast(ectx, source_type, target_type, in);
    if(! converted.error()) {
        out = converted;
        return status::ok;
    }
    auto err = converted.to<expression::error>();
    if(err.kind() == expression::error_kind::unsupported) {
        auto res = status::err_unsupported;
        auto [msg, value_msg] = expression::create_conversion_error_message(ectx);
        set_error(
            ctx,
            error_code::unsupported_runtime_feature_exception,
            msg,
            res
        );
        return res;
    }
    if(err.kind() == expression::error_kind::lost_precision_value_too_long) {
        auto res = status::err_expression_evaluation_failure;
        auto [msg, value_msg] = expression::create_conversion_error_message(ectx);
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
    auto [msg, value_msg] = expression::create_conversion_error_message(ectx);
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

bool to_require_conversion(
    takatori::type::data const& source_type,
    takatori::type::data const& target_type
) {
    // for time and timestamp, we don't require conversion even if they are different offset value
    // since currently only UTC offset is supported, so offset=true/false doesn't make difference
    if(source_type.kind() == takatori::type::type_kind::time_of_day) {
        return target_type.kind() != takatori::type::type_kind::time_of_day;
    }
    if(source_type.kind() == takatori::type::type_kind::time_point) {
        return target_type.kind() != takatori::type::type_kind::time_point;
    }
    return source_type != target_type;
}

}  // namespace jogasaki::executor::conv
