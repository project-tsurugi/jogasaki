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
#include "find_context.h"

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/iterator.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

find_context::find_context(
    class abstract::task_context* ctx,
    variable_table& input_variables,
    variable_table& output_variables,
    std::unique_ptr<kvs::storage> stg,
    std::unique_ptr<kvs::storage> secondary_stg,
    transaction_context* tx,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource
) :
    context_base(ctx, input_variables, output_variables, resource, varlen_resource),
    stg_(std::move(stg)),
    secondary_stg_(std::move(secondary_stg)),
    tx_(tx)
{}

operator_kind find_context::kind() const noexcept {
    return operator_kind::find;
}

void find_context::release() {
    //TODO
}

transaction_context* find_context::transaction() const noexcept {
    return tx_;
}

}


