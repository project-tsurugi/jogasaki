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
#include "mirror_container.h"

namespace jogasaki::plan {

void mirror_container::set(mirror_container::step_index index, mirror_container::variable_definition def) noexcept {
    variable_definitions_[index] = std::move(def);
}

mirror_container::variable_definition const& mirror_container::at(mirror_container::step_index index) const noexcept {
    return variable_definitions_.at(index);
}

}
