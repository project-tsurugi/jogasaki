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

#include <string_view>
#include <cstdlib>

namespace jogasaki::model {

enum class step_kind {
    process,
    forward,
    broadcast,
    group,
    aggregate,
    deliver,
};

constexpr inline std::string_view to_string_view(step_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = step_kind;
    switch (value) {
        case kind::process: return "process"sv;
        case kind::forward: return "forward"sv;
        case kind::broadcast: return "broadcast"sv;
        case kind::group: return "group"sv;
        case kind::aggregate: return "aggregate"sv;
        case kind::deliver: return "deliver"sv;
    }
    std::abort();
}

}
