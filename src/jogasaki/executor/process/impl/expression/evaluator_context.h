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

#include "jogasaki/executor/diagnostic_record.h"

#include <takatori/util/enum_set.h>

#include "error.h"


namespace jogasaki::executor::process::impl::expression {

/**
 * @brief cast loss policy
 */
enum class cast_loss_policy : std::size_t {
    ignore,
    floor,
    ceil,
    unknown,
    warn,
    error,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(cast_loss_policy value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case cast_loss_policy::ignore: return "ignore"sv;
        case cast_loss_policy::floor: return "floor"sv;
        case cast_loss_policy::ceil: return "ceil"sv;
        case cast_loss_policy::unknown: return "unknown"sv;
        case cast_loss_policy::warn: return "warn"sv;
        case cast_loss_policy::error: return "error"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, cast_loss_policy value) {
    return out << to_string_view(value);
}

/**
 * @brief range error policy
 */
enum class range_error_policy : std::size_t {
    ignore,
    wrap,
    warning,
    error,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(range_error_policy value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case range_error_policy::ignore: return "ignore"sv;
        case range_error_policy::wrap: return "wrap"sv;
        case range_error_policy::warning: return "warning"sv;
        case range_error_policy::error: return "error"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, range_error_policy value) {
    return out << to_string_view(value);
}

/**
 * @brief class representing evaluation error
 */
class evaluator_context {
public:
    using error_type = diagnostic_record<error_kind>;

    /**
     * @brief create undefined object
     */
    evaluator_context() = default;

    /**
     * @brief accessor for cast loss policy
     */
    [[nodiscard]] cast_loss_policy get_cast_loss_policy() const noexcept {
        return cast_loss_policy_;
    }

    /**
     * @brief setter for cast loss policy
     */
    evaluator_context& set_cast_loss_policy(cast_loss_policy arg) noexcept {
        cast_loss_policy_ = arg;
        return *this;
    }

    /**
     * @brief accessor for range error policy
     */
    [[nodiscard]] range_error_policy get_range_error_policy() const noexcept {
        return range_error_policy_;
    }

    /**
     * @brief setter for range error policy
     */
    evaluator_context& set_range_error_policy(range_error_policy arg) noexcept {
        range_error_policy_ = arg;
        return *this;
    }

    /**
     * @brief add error for reporting
     */
    evaluator_context& add_error(error_type arg) {
        errors_.emplace_back(std::move(arg));
        return *this;
    }

    /**
     * @brief accessor for errors
     */
    [[nodiscard]] std::vector<error_type> const& errors() const noexcept {
        return errors_;
    }
private:
    cast_loss_policy cast_loss_policy_{cast_loss_policy::ignore};
    range_error_policy range_error_policy_{range_error_policy::ignore};
    std::vector<error_type> errors_{};
};

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output stream
 */
inline std::ostream& operator<<(std::ostream& out, evaluator_context const& value) {
    out <<
        "evaluator_context(" << value.get_cast_loss_policy() <<
        ", " << value.get_range_error_policy();
    for(auto&& e : value.errors()) {
        out << ", " << e;
    }
    out << ")";
    return out;
}

}
