/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "io_exchange_map.h"

#include <jogasaki/executor/exchange/step.h>

namespace jogasaki::executor::process {

void io_exchange_map::reserve_input(std::size_t count) {
    input_entity_.reserve(count);
}

void io_exchange_map::reserve_output(std::size_t count) {
    output_entity_.reserve(count);
}

void io_exchange_map::add_input(std::size_t index, exchange::step* s) {
    if(input_entity_.size() <= index) {
        input_entity_.resize(index + 1);
    }
    input_entity_[index] = s;
}

void io_exchange_map::add_output(std::size_t index, io_exchange_map::output_exchange* s) {
    if(output_entity_.size() <= index) {
        output_entity_.resize(index + 1);
    }
    output_entity_[index] = s;
}

void io_exchange_map::set_external_output(io_exchange_map::external_output_operator* s) {
    external_output_entity_ = s;
}

io_exchange_map::input_exchange* const& io_exchange_map::input_at(std::size_t index) const {
    return input_entity_.at(index);
}

io_exchange_map::output_exchange* const& io_exchange_map::output_at(std::size_t index) const {
    return output_entity_.at(index);
}

io_exchange_map::external_output_operator const* io_exchange_map::external_output() const {
    return external_output_entity_;
}

std::size_t io_exchange_map::input_count() const noexcept {
    return input_entity_.size();
}

std::size_t io_exchange_map::output_count() const noexcept {
    return output_entity_.size();
}

}  // namespace jogasaki::executor::process
