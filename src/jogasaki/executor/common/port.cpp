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
#include "port.h"

#include <utility>
#include <vector>

#include <jogasaki/model/port.h>
#include <jogasaki/model/step.h>

namespace jogasaki::executor::common {

port::port(
    port_direction direction,
    port_kind kind,
    model::step* owner
) noexcept :
    direction_(direction),
    kind_(kind),
    owner_(owner)
{}

sequence_view<model::port* const> port::opposites() const {
    return opposites_;
}

void port::set_opposites(std::vector<model::port*>&& arg) {
    opposites_ = std::move(arg);
}

void port::owner(model::step* arg) {
    owner_ = arg;
}

port_kind port::kind() const {
    return kind_;
}

port_direction port::direction() const {
    return direction_;
}

model::step* const& port::owner() const {
    return owner_;
}

void port::add_opposite(port* target) {
    opposites_.emplace_back(target);
    target->opposites_.emplace_back(this);
}

}

