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
#include "write_existing_context.h"

#include <memory>
#include <utility>
#include <vector>

#include <jogasaki/index/primary_context.h>
#include <jogasaki/index/secondary_context.h>
#include <jogasaki/kvs/storage.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

write_existing_context::write_existing_context(
    class abstract::task_context* ctx,
    variable_table& variables,
    std::unique_ptr<kvs::storage> stg,
    transaction_context* tx,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource,
    std::vector<index::secondary_context> secondary_contexts
) :
    context_base(ctx, variables, resource, varlen_resource),
    tx_(tx),
    primary_context_(
        std::move(stg),
        std::move(key_meta),
        std::move(value_meta),
        req_context()
    ),
    secondary_contexts_(std::move(secondary_contexts))
{}

operator_kind write_existing_context::kind() const noexcept {
    return operator_kind::write_existing;
}

void write_existing_context::release() {
    //no-op
}

transaction_context* write_existing_context::transaction() const noexcept {
    return tx_;
}

index::primary_context& write_existing_context::primary_context() noexcept {
    return primary_context_;
}

}  // namespace jogasaki::executor::process::impl::ops
