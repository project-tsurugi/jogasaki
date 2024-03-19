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
#pragma once

#include <cstddef>
#include <decimal.hh>

#include <takatori/decimal/triple.h>

#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>

namespace jogasaki {

using takatori::decimal::triple;

triple make_triple(std::string_view str, std::optional<std::size_t> precision ={}, std::optional<std::size_t> scale ={}) {
    decimal::Decimal value{std::string{str}};
    return static_cast<triple>(value);
}

}  // namespace jogasaki
