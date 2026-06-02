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
#include "context_base.h"

#include <utility>

#include <glog/logging.h>

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/context_helper.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::executor::process::impl::ops {

context_base::context_base(
    abstract::task_context* context,
    variables_view variables,
    memory::lifo_paged_memory_resource* resource,
    memory::lifo_paged_memory_resource* varlen_resource
) :
    task_context_(context),
    variables_(variables),
    resource_(resource),
    varlen_resource_(varlen_resource)
{}

impl::variables_view context_base::variables() const noexcept {
    return variables_;
}

class abstract::task_context& context_base::task_context() noexcept {
    return *task_context_;
}

context_base::memory_resource* context_base::resource() const noexcept {
    return resource_;
}

context_base::memory_resource* context_base::varlen_resource() const noexcept {
    return varlen_resource_;
}

context_state context_base::state() const noexcept {
    return state_;
}

void context_base::state(context_state state) noexcept {
    if (state_ == state) {
        return;
    }
    VLOG_LP(log_trace) << "context state transition [" << kind() << "]: "
        << to_string_view(state_) << " -> " << to_string_view(state);
    state_ = state;
}

class request_context* context_base::req_context() noexcept {
    context_helper h{*task_context_};
    return h.req_context();
}

void context_base::abort() noexcept {
    state(context_state::aborted);
}

void context_base::dump() const noexcept {
    std::cerr << "context_base:\n"
       << "  " << std::left << std::setw(22) << "task_context:"
       << std::hex << (task_context_ ? task_context_ : nullptr) << "\n"
       << "  " << std::setw(22) << "variables:"
       << (variables_ ? "valid" : "null") << "\n"
       << "  " << std::setw(22) << "block_index:"
       << std::dec << variables_.block_index() << "\n";
    auto const* tbl = variables_.current_table();
    if (tbl != nullptr) {
        tbl->dump("    ");
    }
    std::cerr << "  " << std::setw(22) << "resource:"
       << std::hex << (resource_ ? resource_ : nullptr) << "\n"
       << "  " << std::setw(22) << "varlen_resource:"
       << (varlen_resource_ ? varlen_resource_ : nullptr) << "\n"
       << "  " << std::setw(22) << "state:"
       << to_string_view(state_) << std::endl;
}

}  // namespace jogasaki::executor::process::impl::ops
