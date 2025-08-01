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
#include "project_context.h"

#include <jogasaki/executor/process/impl/ops/context_base.h>

namespace jogasaki::executor::process::impl::ops {

project_context::project_context(
    abstract::task_context* ctx,
    variable_table& variables,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource
) :
    context_base(ctx, variables, resource, varlen_resource)
{}

operator_kind project_context::kind() const noexcept {
    return operator_kind::project;
}

void project_context::release() {
    // no-op
}

}


