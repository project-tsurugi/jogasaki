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

#include <takatori/util/enum_set.h>

namespace jogasaki {

enum class commit_response_kind : std::int32_t {
    undefined = -1,
    accepted = 0,
    available,
    stored,
    propagated
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(commit_response_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = commit_response_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::accepted: return "accepted"sv;
        case kind::available: return "available"sv;
        case kind::stored: return "stored"sv;
        case kind::propagated: return "propagated"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, commit_response_kind value) {
    return out << to_string_view(value);
}

/// @brief a set of commit_response_kind.
using commit_response_kind_set = takatori::util::enum_set<
        commit_response_kind,
        commit_response_kind::undefined,
        commit_response_kind::propagated>;

}  // namespace jogasaki
