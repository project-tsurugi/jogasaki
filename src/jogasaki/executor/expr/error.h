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

#include <cstdlib>
#include <ostream>
#include <string_view>
#include <type_traits>

#include <takatori/util/enum_set.h>

namespace jogasaki::executor::expr {

/**
 * @brief error kind
 */
enum class error_kind : std::size_t {
    ///@brief error kind is undefined or unknown
    undefined = 0,

    ///@brief error on arithmetic operation during evaluation
    arithmetic_error,

    ///@brief value overflows
    overflow,

    ///@brief conversion failure due to lost precision and the policy not allowing it
    lost_precision,

    ///@brief special case of lost_precision that has to truncate the variable-length value
    lost_precision_value_too_long,

    ///@brief string or other representation's format error
    format_error,

    ///@brief io error in lob expression evaluation
    lob_file_io_error,

    ///@brief invalid lob reference occurred in lob expression evaluation
    lob_reference_invalid,

    ///@brief error occurred and its error info. is provided via evaluator_context
    error_info_provided,

    ///@brief unsupported features used in the expression
    unsupported,

    ///@brief unexpected error in the expression
    unknown,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(error_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = error_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::arithmetic_error: return "arithmetic_error"sv;
        case kind::overflow: return "overflow"sv;
        case kind::lost_precision: return "lost_precision"sv;
        case kind::lost_precision_value_too_long: return "lost_precision_value_too_long"sv;
        case kind::format_error: return "format_error"sv;
        case kind::lob_file_io_error: return "lob_file_io_error"sv;
        case kind::lob_reference_invalid: return "lob_reference_invalid"sv;
        case kind::error_info_provided: return "error_info_provided"sv;
        case kind::unsupported: return "unsupported"sv;
        case kind::unknown: return "unknown"sv;
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

/// @brief a set of error_kind.
using error_kind_set = takatori::util::enum_set<
    error_kind,
    error_kind::undefined,
    error_kind::unknown>;

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

}  // namespace jogasaki::executor::expr
