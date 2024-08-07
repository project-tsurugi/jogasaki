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
#include "scalar_function_repository.h"

#include <utility>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/function/scalar_function_info.h>

namespace jogasaki::executor::function {

using takatori::util::maybe_shared_ptr;

void scalar_function_repository::add(std::size_t id, maybe_shared_ptr<scalar_function_info> info) {
    map_.emplace(id, std::move(info));
}

scalar_function_info const* scalar_function_repository::find(std::size_t id) const noexcept {
    if (map_.count(id) == 0) return {};
    return map_.at(id).get();
}

void scalar_function_repository::clear() noexcept {
    map_.clear();
}

std::size_t scalar_function_repository::size() const noexcept {
    return map_.size();
}

}
