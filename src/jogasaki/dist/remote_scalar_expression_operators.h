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

#include <optional>
#include <string_view>

#include <takatori/scalar/unary_operator.h>
#include <takatori/scalar/binary_operator.h>
#include <takatori/scalar/comparison_operator.h>
#include <takatori/scalar/cast_loss_policy.h>

namespace jogasaki::dist {

[[nodiscard]] inline std::optional<takatori::scalar::unary_operator> remote_unary_operator(std::string_view name) {
    using kind = takatori::scalar::unary_operator;
    if (name == "plus") {
        return kind::plus;
    }
    if (name == "sign_inversion") {
        return kind::sign_inversion;
    }
    if (name == "length") {
        return kind::length;
    }
    if (name == "conditional_not") {
        return kind::conditional_not;
    }
    if (name == "is_null") {
        return kind::is_null;
    }
    if (name == "is_true") {
        return kind::is_true;
    }
    if (name == "is_false") {
        return kind::is_false;
    }
    if (name == "is_unknown") {
        return kind::is_unknown;
    }
    return {};
}

[[nodiscard]] inline std::optional<takatori::scalar::binary_operator> remote_binary_operator(std::string_view name) {
    using kind = takatori::scalar::binary_operator;
    if (name == "add") {
        return kind::add;
    }
    if (name == "subtract") {
        return kind::subtract;
    }
    if (name == "multiply") {
        return kind::multiply;
    }
    if (name == "divide") {
        return kind::divide;
    }
    if (name == "remainder") {
        return kind::remainder;
    }
    if (name == "concat") {
        return kind::concat;
    }
    if (name == "conditional_and") {
        return kind::conditional_and;
    }
    if (name == "conditional_or") {
        return kind::conditional_or;
    }
    return {};
}

[[nodiscard]] inline std::optional<takatori::scalar::comparison_operator> remote_comparison_operator(std::string_view name) {
    using kind = takatori::scalar::comparison_operator;
    if (name == "equal") {
        return kind::equal;
    }
    if (name == "not_equal") {
        return kind::not_equal;
    }
    if (name == "less") {
        return kind::less;
    }
    if (name == "less_equal") {
        return kind::less_equal;
    }
    if (name == "greater") {
        return kind::greater;
    }
    if (name == "greater_equal") {
        return kind::greater_equal;
    }
    if (name == "is_not_distinct_from") {
        return kind::is_not_distinct_from;
    }
    if (name == "is_distinct_from") {
        return kind::is_distinct_from;
    }
    return {};
}

[[nodiscard]] inline std::optional<takatori::scalar::cast_loss_policy> remote_cast_loss_policy(std::string_view name) {
    using kind = takatori::scalar::cast_loss_policy;
    if (name == "ignore") {
        return kind::ignore;
    }
    if (name == "floor") {
        return kind::floor;
    }
    if (name == "ceil") {
        return kind::ceil;
    }
    if (name == "unknown") {
        return kind::unknown;
    }
    if (name == "warn") {
        return kind::warn;
    }
    if (name == "error") {
        return kind::error;
    }
    return {};
}

} // namespace jogasaki::dist
