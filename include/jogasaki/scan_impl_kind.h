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

#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <string_view>

namespace jogasaki {

/**
 * @brief the implementation of the transaction engine scan used by the scan operator
 */
enum class scan_impl_kind : std::int32_t {
    /**
     * @brief choose automatically
     * @details iterator based scan is used for the statement without write operations, and the legacy scan is used
     * for the statement with write operations.
     */
    automatic = 0,

    /**
     * @brief always use the legacy scan
     */
    legacy = 1,

    /**
     * @brief always use the iterator based scan
     * @attention this is for development and testing. The statements with write operations can hit
     * Halloween problem (tsurugi-issues#1491).
     */
    iscan = 2,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(scan_impl_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = scan_impl_kind;
    switch (value) {
        case kind::automatic: return "auto"sv;
        case kind::legacy: return "legacy"sv;
        case kind::iscan: return "iscan"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, scan_impl_kind value) {
    return out << to_string_view(value);
}

}  // namespace jogasaki
