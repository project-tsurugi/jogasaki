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
#include <cstdlib>
#include <ostream>
#include <string_view>

namespace jogasaki::executor::file {

enum class time_unit_kind {
    unspecified,
    nanosecond,
    microsecond,
    millisecond,
    second,  // not yet supported for dump
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(time_unit_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = time_unit_kind;
    switch (value) {
        case kind::unspecified: return "unspecified"sv;
        case kind::nanosecond: return "nanosecond"sv;
        case kind::microsecond: return "microsecond"sv;
        case kind::millisecond: return "millisecond"sv;
        case kind::second: return "second"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, time_unit_kind value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki::executor::file
