/*
 * Copyright 2018-2020 tsurugi project.
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

#include <takatori/util/fail.h>
#include <takatori/util/enum_set.h>

namespace jogasaki::executor::process::impl::expression {

using takatori::util::fail;

/**
 * @brief error kind
 */
enum class error_kind : std::size_t {
    undefined = 0,
    arithmetic_error,
    overflow,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(error_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = error_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::arithmetic_error: return "arithmetic_error"sv;
        case kind::overflow: return "overflow"sv;
    }
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, error_kind value) {
    return out << to_string_view(value);
}

/// @brief a set of error_kind.
using error_kind_set = takatori::util::enum_set<
    error_kind,
    error_kind::undefined,
    error_kind::overflow>;

/**
 * @brief class representing evaluation error
 */
class error {
public:
    /**
     * @brief create undefined object
     */
    error() = default;

    /**
     * @brief create new object
     */
    explicit error(error_kind kind) noexcept;

    /**
     * @brief accessor for error kind
     */
    [[nodiscard]] error_kind kind() const noexcept;

private:
    error_kind kind_{error_kind::undefined};
};

static_assert(std::is_trivially_copyable_v<error>);

}
