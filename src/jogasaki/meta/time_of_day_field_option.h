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

#include <cstddef>
#include <ostream>

namespace jogasaki::meta {

struct time_of_day_field_option {
    time_of_day_field_option() = default;

    explicit time_of_day_field_option(std::int64_t tz_min_offset) :
        tz_min_offset_(tz_min_offset)
    {}

    std::int64_t tz_min_offset_{};  //NOLINT
};

bool operator==(time_of_day_field_option const& a, time_of_day_field_option const& b) noexcept;

std::ostream& operator<<(std::ostream& out, time_of_day_field_option const& value);

} // namespace

