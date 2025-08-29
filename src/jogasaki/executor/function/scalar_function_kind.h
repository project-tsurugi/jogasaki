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

namespace jogasaki::executor::function {

/**
 * @brief field type kind
 */
enum class scalar_function_kind : std::size_t {
    undefined = 0,
    octet_length,
    current_date,
    localtime,
    current_timestamp,
    localtimestamp,
    substring,
    upper,
    lower,
    character_length,
    char_length,
    abs,
    position,
    mod,
    substr,
    ceil,
    floor,
    round,
    encode,
    decode,
    rtrim,
    ltrim,
    user_defined
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(scalar_function_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = scalar_function_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::octet_length: return "octet_length"sv;
        case kind::current_date: return "current_date"sv;
        case kind::localtime: return "localtime"sv;
        case kind::current_timestamp: return "current_timestamp"sv;
        case kind::localtimestamp: return "localtimestamp"sv;
        case kind::substring: return "substring"sv;
        case kind::upper: return "upper"sv;
        case kind::lower: return "lower"sv;
        case kind::character_length: return "character_length"sv;
        case kind::char_length: return "char_length"sv;
        case kind::abs: return "abs"sv;
        case kind::position: return "position"sv;
        case kind::mod: return "mod"sv;
        case kind::substr: return "substr"sv;
        case kind::ceil: return "ceil"sv;
        case kind::floor: return "floor"sv;
        case kind::round: return "round"sv;
        case kind::encode: return "encode"sv;
        case kind::decode: return "decode"sv;
        case kind::rtrim: return "rtrim"sv;
        case kind::ltrim: return "ltrim"sv;
        case kind::user_defined: return "user_defined"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, scalar_function_kind value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki::executor::function
