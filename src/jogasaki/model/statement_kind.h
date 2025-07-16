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

namespace jogasaki::model {

/**
 * @brief field type kind
 */
enum class statement_kind : std::size_t {
    execute,
    write,
    create_table,
    drop_table,
    create_index,
    drop_index,
    empty,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(statement_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = statement_kind;
    switch (value) {
        case kind::execute: return "execute"sv;
        case kind::write: return "write"sv;
        case kind::create_table: return "create_table"sv;
        case kind::drop_table: return "drop_table"sv;
        case kind::create_index: return "create_index"sv;
        case kind::drop_index: return "drop_index"sv;
        case kind::empty: return "empty"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, statement_kind value) {
    return out << to_string_view(value);
}

} // namespace

