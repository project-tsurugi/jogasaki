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
#include "any.h"

namespace jogasaki::data {

any::operator bool() const noexcept {
    return !empty() && !error();
}

bool any::empty() const noexcept {
    return body_.index() == 0;
}

bool any::error() const noexcept {
    return body_.index() == 1;
}

std::size_t any::type_index() const noexcept {
    return body_.index();
}
}


