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

#include <vector>

#include <yugawara/storage/index.h>

#include <jogasaki/kvs/coder.h>
#include <jogasaki/common_types.h>
#include <jogasaki/executor/process/impl/expression/single_function_evaluator.h>

namespace jogasaki::executor::process::impl::ops {

enum class default_value_kind : std::size_t {
    nothing,
    immediate,
    sequence,
    function,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(default_value_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = default_value_kind;
    switch (value) {
        case kind::nothing: return "nothing"sv;
        case kind::immediate: return "immediate"sv;
        case kind::sequence: return "sequence"sv;
        case kind::function: return "function"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, default_value_kind value) {
    return out << to_string_view(value);
}

struct cache_align default_value_property {

    default_value_property() = default;

    /**
     * @brief create new write field
     */
    default_value_property(
        default_value_kind kind,
        data::any immediate_value,  //NOLINT
        sequence_definition_id def_id,
        yugawara::function::configurable_provider const* functions = nullptr
    ) :
        kind_(kind),
        immediate_value_(immediate_value),
        def_id_(def_id),
        function_(
            kind == default_value_kind::function ? expression::single_function_evaluator(def_id, *functions)
                                                 : expression::single_function_evaluator{}
        ) {}

    // default value properties (valid if exists_ = false)
    default_value_kind kind_{};  //NOLINT
    data::any immediate_value_{};   //NOLINT
    sequence_definition_id def_id_{};  //NOLINT
    expression::single_function_evaluator function_{};  //NOLINT
};

}  // namespace jogasaki::executor::process::impl::ops
