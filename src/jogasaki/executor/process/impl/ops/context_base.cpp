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
#include "context_base.h"

#include <utility>

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/context_helper.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>

namespace jogasaki::executor::process::impl::ops {

context_base::context_base(
    abstract::task_context* context,
    variable_table& input_variables,
    variable_table& output_variables,
    memory::lifo_paged_memory_resource* resource,
    memory::lifo_paged_memory_resource* varlen_resource
) :
    task_context_(context),
    input_variables_(std::addressof(input_variables)),
    output_variables_(std::addressof(output_variables)),
    resource_(resource),
    varlen_resource_(varlen_resource)
{}

context_base::context_base(
    abstract::task_context* context,
    variable_table& variables,
    memory::lifo_paged_memory_resource* resource,
    memory::lifo_paged_memory_resource* varlen_resource
) :
    context_base(
        context,
        variables,
        variables,
        resource,
        varlen_resource
    )
{}

variable_table& context_base::output_variables() const noexcept {
    return *output_variables_;
}

void context_base::output_variables(variable_table& variables) noexcept {
    output_variables_ = std::addressof(variables);
}

variable_table& context_base::input_variables() const noexcept {
    return *input_variables_;
}

void context_base::input_variables(variable_table& variables) noexcept {
    input_variables_ = std::addressof(variables);
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
    state_ = state;
}

class request_context* context_base::req_context() noexcept {
    context_helper h{*task_context_};
    return h.req_context();
}

void context_base::abort() noexcept {
    state(context_state::abort);
}

std::ostream& operator<<(std::ostream& os, const context_base& ctx) {
    os << "context_base:\n";

    os << "  " << std::left << std::setw(22) << "task_context:"
       << std::hex << (ctx.task_context_ ? ctx.task_context_ : nullptr) << "\n";

    os << "  " << std::left << std::setw(22) << "input_variables:"
       << std::hex << (ctx.input_variables_ ? ctx.input_variables_ : nullptr) << "\n";

    if (ctx.input_variables_) {
        ctx.input_variables_->debug_print(os, "    ");
    }

    os << "  " << std::left << std::setw(22) << "output_variables:"
       << std::hex << (ctx.output_variables_ ? ctx.output_variables_ : nullptr) << "\n";

    if (ctx.output_variables_) {
        ctx.output_variables_->debug_print(os, "    ");
    }

    os << "  " << std::left << std::setw(22) << "resource:"
       << std::hex << (ctx.resource_ ? ctx.resource_ : nullptr) << "\n";

    os << "  " << std::left << std::setw(22) << "varlen_resource:"
       << std::hex << (ctx.varlen_resource_ ? ctx.varlen_resource_ : nullptr) << "\n";

    os << "  " << std::left << std::setw(22) << "state:"
       << static_cast<int>(ctx.state_) << "\n";

    return os;
}

}
