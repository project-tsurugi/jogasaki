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
#include "scan_context.h"

#include <utility>

#include <jogasaki/executor/process/impl/scan_info.h>
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
    impl::scan_info const* scan_info,
    context_base::memory_resource* resource,
    context_base::memory_resource* varlen_resource
) :
    context_base(ctx, variables, resource, varlen_resource),
    stg_(std::move(stg)),
    secondary_stg_(std::move(secondary_stg)),
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

transaction_context* scan_context::transaction() const noexcept {
    return tx_;
}

std::ostream& operator<<(std::ostream& os, const scan_context& sc) {
    os << static_cast<const context_base&>(sc);
    os << "  scan_context:\n";

    os << "    " << std::left << std::setw(20) << "stg:"
       << std::hex << (sc.stg_ ? sc.stg_.get() : nullptr) << "\n";

    os << "    " << std::left << std::setw(20) << "secondary_stg:"
       << std::hex << (sc.secondary_stg_ ? sc.secondary_stg_.get() : nullptr) << "\n";

    os << "    " << std::left << std::setw(20) << "transaction_context:"
       << std::hex << (sc.tx_ ? sc.tx_ : nullptr) << "\n";

    os << "    " << std::left << std::setw(20) << "iterator:"
       << std::hex << (sc.it_ ? sc.it_.get() : nullptr) << "\n";

    os << "    " << std::left << std::setw(20) << "scan_info:"
       << std::hex << (sc.scan_info_ ? sc.scan_info_ : nullptr) << "\n";

    os << "    " << std::left << std::setw(20) << "key_begin_size:"
       << sc.key_begin_.size() << "\n";

    os << "    " << std::left << std::setw(20) << "key_end_size:"
       << sc.key_end_.size() << "\n";

    return os;
}

}


