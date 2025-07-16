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
#include "binary_string_value.h"

#include <string>

namespace jogasaki::data {

binary_string_value::binary_string_value(std::string arg) :
    body_(std::move(arg))
{}

binary_string_value::binary_string_value(std::string_view arg) :
    body_(arg)
{}

std::string const& binary_string_value::str() const {
    return body_;
}

}  // namespace jogasaki::data
