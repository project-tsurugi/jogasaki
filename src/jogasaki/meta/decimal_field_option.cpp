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
#include "decimal_field_option.h"

#include <string>

namespace jogasaki::meta {

bool operator==(decimal_field_option const& a, decimal_field_option const& b) noexcept {
    return a.precision_ == b.precision_ && a.scale_ == b.scale_;
}

std::ostream& operator<<(std::ostream& out, decimal_field_option const& value) {
    return out << "decimal(" <<
        (value.precision_.has_value() ? std::to_string(value.precision_.value()) : "*") <<
        ", " <<
        (value.scale_.has_value() ? std::to_string(value.scale_.value()) : "*") <<
        ")";
}

} // namespace

