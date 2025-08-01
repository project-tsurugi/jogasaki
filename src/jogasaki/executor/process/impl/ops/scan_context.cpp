/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <utility>

#include <jogasaki/executor/process/impl/scan_range.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/transaction_context.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

scan_context::scan_context(
    class abstract::task_context* ctx,
    variable_table& variables,
    std::unique_ptr<kvs::storage> stg,
    std::unique_ptr<kvs::storage> secondary_stg,
    transaction_context* tx,
    impl::scan_range const* range,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource,
    kvs::transaction* strand
) :
    context_base(ctx, variables, resource, varlen_resource),
    stg_(std::move(stg)),
    secondary_stg_(std::move(secondary_stg)),
    tx_(tx),
    range_(range),
    strand_(strand)
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

transaction_context* scan_context::transaction() const noexcept {
    return tx_;
}

void scan_context::dump() const noexcept {
    context_base::dump();
    std::cerr << "  scan_context:\n"
       << "    " << std::left << std::setw(20) << "stg:"
       << std::hex << (stg_ ? stg_.get() : nullptr) << "\n"
       << "    " << std::setw(20) << "secondary_stg:"
       << (secondary_stg_ ? secondary_stg_.get() : nullptr) << "\n"
       << "    " << std::setw(20) << "transaction_context:"
       << (tx_ ? tx_ : nullptr) << "\n"
       << "    " << std::setw(20) << "iterator:"
       << (it_ ? it_.get() : nullptr) << "\n";
}

kvs::transaction* scan_context::strand() const noexcept {
    return strand_;
}

} // namespace jogasaki::executor::process::impl::ops
