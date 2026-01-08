/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "apply_context.h"

#include <utility>

namespace jogasaki::executor::process::impl::ops {

apply_context::apply_context(
    class abstract::task_context* ctx,
    variable_table& variables,
    memory_resource* resource,
    memory_resource* varlen_resource
) :
    context_base(ctx, variables, resource, varlen_resource),
    evaluator_context_{varlen_resource, nullptr}
{
    // update transaction context after context_base is initialized
    if (req_context()) {
        evaluator_context_.set_transaction(req_context()->transaction().get());
    }
}

operator_kind apply_context::kind() const noexcept {
    return operator_kind::apply;
}

void apply_context::release() {
    if (stream_) {
        stream_->close();
        stream_.reset();
    }
    has_output_ = false;
}

}  // namespace jogasaki::executor::process::impl::ops
