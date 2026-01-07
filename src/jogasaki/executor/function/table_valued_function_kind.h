/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <ostream>
#include <string_view>

namespace jogasaki::executor::function {

/**
 * @brief represents the kind of table-valued function.
 */
enum class table_valued_function_kind {

    /**
     * @brief undefined function kind.
     */
    undefined,

    /**
     * @brief builtin table-valued function.
     */
    builtin,

    /**
     * @brief user-defined table-valued function (UDTF).
     */
    user_defined,
};

/**
 * @brief returns the string representation of the given kind.
 * @param value the target value
 * @return the string representation
 */
[[nodiscard]] constexpr std::string_view to_string_view(table_valued_function_kind value) noexcept {
    using kind = table_valued_function_kind;
    switch (value) {
        case kind::undefined: return "undefined";
        case kind::builtin: return "builtin";
        case kind::user_defined: return "user_defined";
    }
    return "unknown";
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output stream
 */
inline std::ostream& operator<<(std::ostream& out, table_valued_function_kind value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki::executor::function
