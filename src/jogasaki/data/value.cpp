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
#include "value.h"

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/accessor/text.h>

#include "any.h"

namespace jogasaki::data {

value::operator bool() const noexcept {
    return !empty();
}

bool value::empty() const noexcept {
    return body_.index() == 0;
}

std::size_t value::type_index() const noexcept {
    return body_.index();
}

}  // namespace jogasaki::data
