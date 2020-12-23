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

#include <takatori/util/enum_set.h>

namespace jogasaki::executor::process::impl::ops {

enum class operator_kind : std::size_t {
    unknown = 0,
    emit,
    write_full,
    write_partial,
    scan,
    find,
    take_group,
    take_flat,
    offer,
    filter,
    project,
    take_cogroup,
    join,
    flatten,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
constexpr inline std::string_view to_string_view(operator_kind value) noexcept {
    using namespace std::string_view_literals;
    switch (value) {
        case operator_kind::unknown: return "unknown"sv;
        case operator_kind::emit: return "emit"sv;
        case operator_kind::write_partial: return "write_partial"sv;
        case operator_kind::write_full: return "write_full"sv;
        case operator_kind::scan: return "scan"sv;
        case operator_kind::find: return "find"sv;
        case operator_kind::take_group: return "take_group"sv;
        case operator_kind::take_flat: return "take_flat"sv;
        case operator_kind::offer: return "offer"sv;
        case operator_kind::filter: return "filter"sv;
        case operator_kind::project: return "project"sv;
        case operator_kind::take_cogroup: return "take_cogroup"sv;
        case operator_kind::join: return "join"sv;
        case operator_kind::flatten: return "flatten"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, operator_kind value) {
    return out << to_string_view(value);
}

/// @brief a set of expression_kind.
using operator_kind_set = takatori::util::enum_set<
    operator_kind,
    operator_kind::unknown,
    operator_kind::flatten>;
}
