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

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>

#include <takatori/util/enum_set.h>

namespace jogasaki {

/**
 * @brief transaction type kind
 */
enum class transaction_type_kind : std::int32_t {
    /**
     * @brief unknown type
     */
    unknown = 0,

    /**
     * @brief occ transaction
     */
    occ,

    /**
     * @brief long transaction
     */
    ltx,

    /**
     * @brief read only transaction
     */
    rtx,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(transaction_type_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = transaction_type_kind;
    switch (value) {
        case kind::unknown: return "unknown"sv;
        case kind::occ: return "occ"sv;
        case kind::ltx: return "ltx"sv;
        case kind::rtx: return "rtx"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, transaction_type_kind value) {
    return out << to_string_view(value);
}

/// @brief a set of transaction_type_kind
using transaction_type_kind_set = takatori::util::enum_set<
        transaction_type_kind,
        transaction_type_kind::unknown,
        transaction_type_kind::rtx>;

}

