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
#include "scan_context.h"

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/executor/process/impl/scan_info.h>
#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

scan_context::scan_context(
    class abstract::task_context* ctx,
    variable_table& variables,
    std::unique_ptr<kvs::storage> stg,
    kvs::transaction* tx,
    impl::scan_info const* scan_info,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource
) :
    context_base(ctx, variables, resource, varlen_resource),
    stg_(std::move(stg)),
    tx_(tx),
    scan_info_(scan_info)
{}

operator_kind scan_context::kind() const noexcept {
    return operator_kind::scan;
}

void scan_context::release() {
    if(it_) {
        // TODO revisit the life-time of storage objects
        it_ = nullptr;
    }
}

kvs::transaction* scan_context::transaction() const noexcept {
    return tx_;
}

}


