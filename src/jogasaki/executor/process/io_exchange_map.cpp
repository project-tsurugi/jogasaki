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

std::size_t io_exchange_map::add_input(exchange::step* s) {
    input_entity_.emplace_back(s);
    return input_entity_.size() - 1;
}

std::size_t io_exchange_map::add_output(io_exchange_map::output_exchange* s) {
    output_entity_.emplace_back(s);
    return output_entity_.size() - 1;
}

void io_exchange_map::set_external_output(io_exchange_map::external_output_operator* s) {
    external_output_entity_ = s;
}

std::size_t io_exchange_map::input_index(io_exchange_map::input_exchange* s) {
    for(std::size_t i=0, n=input_entity_.size(); i < n; ++i) {
        if(input_entity_[i] == s) {
            return i;
        }
    }
    return npos;
}

std::size_t io_exchange_map::output_index(io_exchange_map::output_exchange* s) {
    for(std::size_t i=0, n=output_entity_.size(); i < n; ++i) {
        if(output_entity_[i] == s) {
            return i;
        }
    }
    return npos;
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

}


