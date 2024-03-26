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
#include "flow_repository.h"

#include <utility>

#include <takatori/util/fail.h>

#include <jogasaki/model/flow.h>

namespace jogasaki::model {

using takatori::util::fail;

flow_repository::flow_repository(std::size_t size) :
    flows_(size)
{}

void flow_repository::set(
    std::size_t idx,
    std::unique_ptr<flow> arg
) noexcept {
    if (idx >= flows_.size()) fail();
    flows_[idx] = std::move(arg);
}

bool flow_repository::exists(std::size_t idx) const noexcept {
    return flows_[idx] != nullptr;
}

std::size_t flow_repository::size() const noexcept {
    return flows_.size();
}

flow* flow_repository::at(std::size_t idx) const noexcept {
    if (idx >= flows_.size()) return nullptr;
    return flows_.at(idx).get();
}

}

