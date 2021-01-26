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
#include "write_full_context.h"

#include <vector>
#include <memory>

#include <jogasaki/executor/process/step.h>

namespace jogasaki::executor::process::impl::ops {

write_full_context::write_full_context(
    class abstract::task_context* ctx,
    block_scope& variables,
    std::unique_ptr<kvs::storage> stg,
    kvs::transaction* tx,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource
) :
    context_base(ctx, variables, resource, varlen_resource),
    stg_(std::move(stg)),
    tx_(tx)
{}

operator_kind write_full_context::kind() const noexcept {
    return operator_kind::write_full;
}

void write_full_context::release() {
    //no-op
}

kvs::transaction* write_full_context::transaction() const noexcept {
    return tx_;
}

}


