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
#include "variable_value_map.h"

namespace jogasaki::executor::process::impl {

std::size_t value_info::value_offset() const noexcept {
    return value_offset_;
}

std::size_t value_info::nullity_offset() const noexcept {
    return nullity_offset_;
}

variable_value_map::variable_value_map(variable_value_map::entity_type map) noexcept:
    map_(std::move(map))
{}

value_info const& variable_value_map::at(variable_value_map::variable const& var) const {
    return map_.at(var);
}

}


