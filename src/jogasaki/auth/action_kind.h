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

#include <cstdlib>
#include <ostream>
#include <string_view>

#include <takatori/util/enum_set.h>

namespace jogasaki::auth {

/**
 * @brief enum for authorized actions.
 */
enum class action_kind {
    undefined = 0,
    control,
    select,
    insert,
    update,
    delete_,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(action_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = action_kind;
    switch (value) {
        case kind::undefined: return "undefined"sv;
        case kind::control: return "control"sv;
        case kind::select: return "select"sv;
        case kind::insert: return "insert"sv;
        case kind::update: return "update"sv;
        case kind::delete_: return "delete"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, action_kind value) {
    return out << to_string_view(value);
}

/**
 * @brief set of authorized_action_kind
 */
using action_kind_set = takatori::util::enum_set<
    action_kind,
    action_kind::undefined,
    action_kind::delete_
>;

}  // namespace jogasaki::auth
