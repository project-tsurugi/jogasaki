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
#include "flow_container.h"

#include <takatori/util/fail.h>

namespace jogasaki::executor {

using takatori::util::fail;

flow_container::flow_container(std::size_t size) :
    flows_(size)
{}

std::unique_ptr<common::flow>& flow_container::set(
    std::size_t idx,
    std::unique_ptr<common::flow> ctx
) noexcept
{
    if (idx >= flows_.size()) fail();
    flows_[idx] = std::move(ctx);
    return flows_[idx];
}

bool flow_container::exists(std::size_t idx) const noexcept {
    return flows_[idx] != nullptr;
}

std::size_t flow_container::size() const noexcept {
    return flows_.size();
}

common::flow* flow_container::at(std::size_t idx) const noexcept {
    if (idx >= flows_.size()) return nullptr;
    return flows_.at(idx).get();
}

}

