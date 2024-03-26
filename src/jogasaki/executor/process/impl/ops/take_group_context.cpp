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
#include "take_group_context.h"

#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/process/impl/ops/context_base.h>

namespace jogasaki::executor::process::impl::ops {

take_group_context::take_group_context(
    abstract::task_context* ctx,
    variable_table& variables,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource
) :
    context_base(ctx, variables, resource, varlen_resource)
{}


operator_kind take_group_context::kind() const noexcept {
    return operator_kind::take_group;
}

void take_group_context::release() {
    if(reader_) {
        reader_->release();
        reader_ = nullptr;
    }
}

}


