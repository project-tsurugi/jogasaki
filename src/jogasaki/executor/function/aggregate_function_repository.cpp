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
#include "aggregate_function_repository.h"

#include <vector>
#include <set>
#include <memory>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/fail.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/executor/function/aggregate_function_info.h>

namespace jogasaki::executor::function {

using takatori::util::maybe_shared_ptr;

void aggregate_function_repository::add(std::size_t id, maybe_shared_ptr<aggregate_function_info> info) {
    map_.emplace(id, info);
}

aggregate_function_info const *aggregate_function_repository::find(std::size_t id) {
    if (map_.count(id) == 0) return {};
    return map_[id].get();
}

}
