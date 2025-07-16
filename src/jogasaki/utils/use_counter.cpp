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
#include "use_counter.h"

#include <memory>
#include <utility>

namespace jogasaki::utils {

use_counter::use_counter(use_counter&& other) noexcept:
    entity_(std::move(other.entity_))
{
    other.reset();
}

use_counter& use_counter::operator=(use_counter&& other) noexcept {
    entity_ = std::move(other.entity_);
    other.reset();
    return *this;
}

std::size_t use_counter::count() const noexcept {
    return entity_.use_count();
}

void use_counter::reset() {
    entity_ = std::make_shared<int>(0);
}

use_counter::use_counter() {
    reset();
}
}

