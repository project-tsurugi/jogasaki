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

#include <cstddef>
#include <string>

#include <takatori/util/enum_tag.h>

namespace jogasaki {

enum class internal_event_kind {
    activate,
    prepare,
    consume,
    deactivate,
    propagate_downstream_completing,
};

[[nodiscard]] inline constexpr std::string_view to_string_view(internal_event_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = internal_event_kind;
    switch (value) {
        case kind::activate: return "activate"sv;
        case kind::prepare: return "prepare"sv;
        case kind::consume: return "consume"sv;
        case kind::deactivate: return "deactivate"sv;
        case kind::propagate_downstream_completing: return "propagate_downstream_completing"sv;
    }
    std::abort();
}

class internal_event {
public:
    using identity_type = std::size_t;
    internal_event() = default;
    internal_event(internal_event_kind kind, identity_type target) : kind_(kind), target_(target) {}

    [[nodiscard]] internal_event_kind kind() {
        return kind_;
    }
    [[nodiscard]] identity_type target() {
        return target_;
    }

private:
    internal_event_kind kind_;
    identity_type target_;
};

static_assert(std::is_trivially_copyable_v<internal_event>);

template<class Callback, class... Args>
inline auto dispatch(Callback&& callback, internal_event_kind tag_value, Args&&... args) {
    using kind = internal_event_kind;
    switch (tag_value) {
        case kind::activate: return takatori::util::enum_tag_callback<kind::activate>(std::forward<Callback>(callback), std::forward<Args>(args)...);
        case kind::prepare: return takatori::util::enum_tag_callback<kind::prepare>(std::forward<Callback>(callback), std::forward<Args>(args)...);
        case kind::consume: return takatori::util::enum_tag_callback<kind::consume>(std::forward<Callback>(callback), std::forward<Args>(args)...);
        case kind::deactivate: return takatori::util::enum_tag_callback<kind::deactivate>(std::forward<Callback>(callback), std::forward<Args>(args)...);
        case kind::propagate_downstream_completing: return takatori::util::enum_tag_callback<kind::propagate_downstream_completing>(std::forward<Callback>(callback), std::forward<Args>(args)...);
    }
    std::abort();
}

}

