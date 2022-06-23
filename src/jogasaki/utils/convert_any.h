/*
 * Copyright 2018-2020 tsurugi project.
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

#include <takatori/util/fail.h>

#include <jogasaki/data/any.h>

namespace jogasaki::utils {

using data::any;
using data::index;

constexpr static std::size_t npos = static_cast<std::size_t>(-1);

// convert the value type contained in any if necessary
bool convert_any(any& a, meta::field_type const& type);

}
