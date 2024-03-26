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
#include "aggregate_group_context.h"

#include <utility>

#include <jogasaki/data/value_store.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

aggregate_group_context::aggregate_group_context(
    abstract::task_context* ctx,
    variable_table& variables,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource,
    std::vector<data::value_store> stores,
    std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> resources,
    std::vector<std::vector<std::reference_wrapper<data::value_store>>> function_arg_stores,
    std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> nulls_resources
) :
    context_base(ctx, variables, resource, varlen_resource),
    stores_(std::move(stores)),
    resources_(std::move(resources)),
    nulls_resources_(std::move(nulls_resources)),
    function_arg_stores_(std::move(function_arg_stores))
{}

operator_kind aggregate_group_context::kind() const noexcept {
    return operator_kind::aggregate_group;
}

void aggregate_group_context::release() {
    //no-op
}

}
