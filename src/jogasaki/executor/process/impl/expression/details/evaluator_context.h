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

#include <type_traits>
#include <string_view>
#include <ostream>

#include <takatori/util/enum_set.h>

namespace jogasaki::executor::process::impl::expression {

/**
 * @brief error kind
 */
enum class loss_policy : std::size_t {
    undefined = 0,
    arithmetic_error,
    overflow,
    cast_failure,
    format_error,
    unsupported,
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
        case kind::cast_failure: return "cast_failure"sv;
        case kind::format_error: return "format_error"sv;
        case kind::unsupported: return "unsupported"sv;
    }
    std::abort();
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

/**
 * @brief class representing evaluation error
 */
class evaluator_context {
public:
    /**
     * @brief create undefined object
     */
    evaluator_context() = default;

    /**
     * @brief create new object
     */
    explicit evaluator_context(error_kind kind) noexcept;

    /**
     * @brief accessor for error kind
     */
    [[nodiscard]] error_kind kind() const noexcept;

    /**
     * @brief appends string representation of the given value.
     * @param out the target output
     * @param value the target value
     * @return the output stream
     */
    friend std::ostream& operator<<(std::ostream& out, error const& value) {
        return out << value.kind();
    }
private:
    error_kind kind_{error_kind::undefined};
};

static_assert(std::is_trivially_copyable_v<error>);

/**
 * @brief equality comparison operator
 */
inline bool operator==(error const& a, error const& b) noexcept {
    return a.kind() == b.kind();
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(error const& a, error const& b) noexcept {
    return !(a == b);
}

}
