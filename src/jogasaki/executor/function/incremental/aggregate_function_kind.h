/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <takatori/util/enum_set.h>

namespace jogasaki::executor::function::incremental {

/**
 * @brief field type kind
 */
enum class aggregate_function_kind : std::size_t {
    undefined = 0,
    sum,
    count,
    count_rows,
    max,
    min,
    avg
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(aggregate_function_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = aggregate_function_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::sum: return "sum"sv;
        case kind::count: return "count"sv;
        case kind::count_rows: return "count_rows"sv;
        case kind::max: return "max"sv;
        case kind::min: return "min"sv;
        case kind::avg: return "avg"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, aggregate_function_kind value) {
    return out << to_string_view(value);
}

} // namespace

