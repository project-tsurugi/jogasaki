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
#include "context_container.h"

#include <takatori/util/fail.h>

#include <jogasaki/executor/process/impl/ops/context_base.h>

namespace jogasaki::executor::process::impl::ops {

using takatori::util::fail;

context_container::context_container(std::size_t size) :
    contexts_(size)
{}

std::unique_ptr<context_base>& context_container::set(
    std::size_t idx,
    std::unique_ptr<context_base> ctx
) noexcept
{
    if (idx >= contexts_.size()) fail();
    contexts_[idx] = std::move(ctx);
    return contexts_[idx];
}

bool context_container::exists(std::size_t idx) const noexcept {
    return contexts_[idx] != nullptr;
}

std::size_t context_container::size() const noexcept {
    return contexts_.size();
}

ops::context_base* context_container::at(std::size_t idx) const noexcept {
    if (idx >= contexts_.size()) return nullptr;
    return contexts_.at(idx).get();
}

}

