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

#include <cstdint>
#include <string_view>
#include <cstdlib>

namespace dc::scheduler {

enum class step_state_kind : std::int32_t {
    uninitialized = 0,
    created,
    activated,
    preparing,
    prepared,
    running,
    completing,
    completed,
    deactivated,
};

inline constexpr std::string_view to_string_view(step_state_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = step_state_kind;
    switch (value) {
        case kind::uninitialized: return "uninitialized"sv;
        case kind::created: return "created"sv;
        case kind::activated: return "activated"sv;
        case kind::preparing: return "preparing"sv;
        case kind::prepared: return "prepared"sv;
        case kind::running: return "running"sv;
        case kind::completing: return "completing"sv;
        case kind::completed: return "completed"sv;
        case kind::deactivated: return "deactivated"sv;
    }
    std::abort();
}

enum class task_state_kind : std::int32_t {
    uninitialized = 0,
    running,
    completed,
};

inline constexpr std::string_view to_string_view(task_state_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = task_state_kind;
    switch (value) {
        case kind::uninitialized: return "uninitialized"sv;
        case kind::running: return "running"sv;
        case kind::completed: return "completed"sv;
    }
    std::abort();
}

inline bool operator<(step_state_kind current, step_state_kind from) {
    return static_cast<std::int32_t>(current) < static_cast<std::int32_t>(from);
}
inline bool operator>(step_state_kind current, step_state_kind from) {
    return from < current;
}
inline bool operator<=(step_state_kind current, step_state_kind from) {
    return !(current > from);
}
inline bool operator>=(step_state_kind current, step_state_kind from) {
    return !(current < from);
}

}
