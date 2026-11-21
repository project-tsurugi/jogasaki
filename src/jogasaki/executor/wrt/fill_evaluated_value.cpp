/*
 * Copyright 2026 Project Tsurugi.
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
#include "fill_evaluated_value.h"
#include <jogasaki/executor/conv/record_assignment.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/details/expression_error.h>
#include <jogasaki/executor/process/impl/variables_view.h>
#include <jogasaki/request_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::executor::wrt {
status fill_evaluated_value(expr::evaluator const& eval, conv::record_assignment const& assignment,
    request_context& context, relay::blob_session_container& blob_session,
    process::impl::variables_view variables, memory::lifo_paged_memory_resource& resource,
    accessor::record_ref target) {
    expr::evaluator_context evaluation{std::addressof(resource), context.transaction().get()};
    evaluation.blob_session(std::addressof(blob_session));
    auto result = eval(evaluation, variables, std::addressof(resource));
    if (result.error()) {
        handle_expression_error(context, result, evaluation);
        return context.error_info()->status();
    }
    return assignment.apply(result, target, std::addressof(context), std::addressof(resource));
}
}
