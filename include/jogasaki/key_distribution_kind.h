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

#include <iomanip>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

namespace jogasaki {

enum class key_distribution_kind : std::int32_t {
    undefined = -1,
    simple = 0,
    uniform = 1,
    sampling = 2,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(key_distribution_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = key_distribution_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::simple: return "simple"sv;
        case kind::uniform: return "uniform"sv;
        case kind::sampling: return "sampling"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, key_distribution_kind value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki
