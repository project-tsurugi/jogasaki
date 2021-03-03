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
#include "aggregate_group_context.h"

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

aggregate_group_context::aggregate_group_context(
    abstract::task_context* ctx,
    block_scope& variables,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource,
    std::vector<data::value_store> stores,
    std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> resources,
    std::vector<std::vector<std::reference_wrapper<data::value_store>>> function_arg_stores
) :
    context_base(ctx, variables, resource, varlen_resource),
    stores_(std::move(stores)),
    resources_(std::move(resources)),
    function_arg_stores_(std::move(function_arg_stores))
{}

operator_kind aggregate_group_context::kind() const noexcept {
    return operator_kind::aggregate_group;
}

void aggregate_group_context::release() {
    //no-op
}

}
