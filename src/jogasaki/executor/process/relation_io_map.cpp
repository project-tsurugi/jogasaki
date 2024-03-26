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
#include "relation_io_map.h"

#include <utility>

namespace jogasaki::executor::process {

relation_io_map::relation_io_map(
    relation_io_map::entity_type input_entity,
    relation_io_map::entity_type output_entity
) :
    input_entity_(std::move(input_entity)),
    output_entity_(std::move(output_entity))
{}

std::size_t relation_io_map::input_index(takatori::descriptor::relation const& arg) const {
    return input_entity_.at(arg);
}

std::size_t relation_io_map::output_index(takatori::descriptor::relation const& arg) const {
    return output_entity_.at(arg);
}

std::size_t relation_io_map::input_count() const noexcept {
    return input_entity_.size();
}

std::size_t relation_io_map::output_count() const noexcept {
    return output_entity_.size();
}

}


