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
#include "character_field_option.h"

namespace jogasaki::meta {

bool operator==(character_field_option const& a, character_field_option const& b) noexcept {
    return a.varying_ == b.varying_ && a.length_ == b.length_;
}

std::ostream& operator<<(std::ostream& out, character_field_option const& value) {
    return out << "character" <<
        (value.varying_ ? " varying" : "") <<
        "(" <<
        (value.length_.has_value() ? std::to_string(value.length_.value()) : "*") <<
        ")";
}

} // namespace

