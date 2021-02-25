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
#include "context_base.h"

#include <jogasaki/executor/process/impl/ops/context_helper.h>

namespace jogasaki::executor::process::impl::ops {

context_base::context_base(
    abstract::task_context* context,
    block_scope& variables,
    memory::lifo_paged_memory_resource* resource,
    memory::lifo_paged_memory_resource* varlen_resource
) :
    task_context_(context),
    variables_(std::addressof(variables)),
    resource_(resource),
    varlen_resource_(varlen_resource)
{}

block_scope& context_base::variables() const noexcept {
    return *variables_;
}

void context_base::variables(block_scope& variables) noexcept {
    variables_ = std::addressof(variables);
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

}
