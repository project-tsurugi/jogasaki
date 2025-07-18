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
#include "field_locator.h"

#include <utility>

#include <jogasaki/meta/field_type.h>

namespace jogasaki::executor::function {

field_locator::field_locator(
    meta::field_type const& type,
    bool nullable,
    std::size_t value_offset,
    std::size_t nullity_offset
) :
    type_(std::addressof(type)),
    nullable_(nullable),
    value_offset_(value_offset),
    nullity_offset_(nullity_offset)
{}

meta::field_type const& field_locator::type() const noexcept {
    return *type_;
}

bool field_locator::nullable() const noexcept {
    return nullable_;
}

std::size_t field_locator::value_offset() const noexcept {
    return value_offset_;
}

std::size_t field_locator::nullity_offset() const noexcept {
    return nullity_offset_;
}

field_locator::operator bool() const noexcept {
    return type_ != nullptr;
}

}
