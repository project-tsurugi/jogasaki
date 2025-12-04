/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "parameter_apply.h"

#include <takatori/type/data.h>

#include <jogasaki/data/any.h>
#include <jogasaki/executor/expr/details/cast_evaluation.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::conv {

using namespace jogasaki::executor::process::impl;

status conduct_parameter_application_conversion(
    takatori::type::data const& source_type,
    takatori::type::data const& target_type,
    data::any const& in,
    data::any& out,
    memory::lifo_paged_memory_resource* resource
) {
    expr::evaluator_context ectx{resource, nullptr}; // parameter application conversion evaluates no function

    // parameter application doesn't lose precision
    ectx.set_loss_precision_policy(expr::loss_precision_policy::ignore);

    out = expr::details::conduct_cast(ectx, source_type, target_type, in);
    return status::ok;
}

}  // namespace jogasaki::executor::conv
