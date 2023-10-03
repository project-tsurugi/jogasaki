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
#include "time_of_day_field_option.h"

namespace jogasaki::meta {

bool operator==(time_of_day_field_option const& a, time_of_day_field_option const& b) noexcept {
    return a.with_offset_ == b.with_offset_;
}

std::ostream& operator<<(std::ostream& out, time_of_day_field_option const& value) {
    return out << "time_of_day(with_offset=" << value.with_offset_ << ")";
}

} // namespace

