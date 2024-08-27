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
#include "info.h"

#include <utility>

#include "jogasaki/common_types.h"

namespace jogasaki::executor::sequence {

info::info(
    sequence_definition_id sequence_definition_id,
    sequence_id sequence_id,
    std::string_view name,
    sequence_value initial_value,
    sequence_value increment,
    sequence_value minimum_value,
    sequence_value maximum_value,
    bool enable_cycle
) :
    sequence_definition_id_(sequence_definition_id),
    sequence_id_(sequence_id),
    name_(name),
    initial_value_(initial_value),
    increment_(increment),
    minimum_value_(minimum_value),
    maximum_value_(maximum_value),
    enable_cycle_(enable_cycle)
{}

info::info(
    sequence_definition_id sequence_definition_id,
    sequence_id sequence_id,
    std::string name
) :
    sequence_definition_id_(sequence_definition_id),
    sequence_id_(sequence_id),
    name_(std::move(name))
{}

sequence_definition_id info::definition_id() const {
    return sequence_definition_id_;
}

sequence_id info::id() const {
    return sequence_id_;
}

std::string_view info::name() const noexcept {
    return name_;
}

sequence_value info::initial_value() const {
    return initial_value_;
}

sequence_value info::increment() const {
    return increment_;
}

sequence_value info::minimum_value() const {
    return minimum_value_;
}

sequence_value info::maximum_value() const {
    return maximum_value_;
}

bool info::cycle() const {
    return enable_cycle_;
}

}  // namespace jogasaki::executor::sequence
