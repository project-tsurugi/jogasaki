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

#include <vector>

#include <yugawara/storage/index.h>
#include <jogasaki/kvs/coder.h>

namespace jogasaki::executor::process::impl::ops {

enum class default_value_kind : std::size_t {
    nothing,
    immediate,
    sequence,
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
        std::string_view default_value,
        sequence_definition_id def_id
    ) :
        kind_(kind),
        default_value_(default_value),
        def_id_(def_id)
    {}

    // default value properties (valid if exists_ = false)
    default_value_kind kind_{};
    data::aligned_buffer default_value_{};
    sequence_definition_id def_id_{};
};

}


