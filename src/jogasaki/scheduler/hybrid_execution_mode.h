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

#include <string_view>
#include <ostream>

namespace jogasaki::scheduler {

enum class hybrid_execution_mode_kind {
    undefined = 0,
    serial,
    stealing,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(hybrid_execution_mode_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = hybrid_execution_mode_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::serial: return "serial"sv;
        case kind::stealing: return "stealing"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream &operator<<(std::ostream &out, hybrid_execution_mode_kind value) {
    return out << to_string_view(value);
}

}

