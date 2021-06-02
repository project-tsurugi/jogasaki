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

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>

namespace jogasaki {

/*
 * @brief external events handled in dag controller
 */
enum class event_kind {
    /*
     * @brief upstream step start sending data to downstream
     * Valid only when downstream step is not blocking exchange
     */
    providing,

    /*
     * @brief a task completed
     */
    task_completed,

    /*
     * @brief early completion has been requested
     */
    completion_instructed,
};

[[nodiscard]] inline constexpr std::string_view to_string_view(event_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = event_kind;
    switch (value) {
        case kind::providing: return "providing"sv;
        case kind::task_completed: return "task_completed"sv;
        case kind::completion_instructed: return "completion_instructed"sv;
    }
    std::abort();
}

template<class Callback, class... Args>
inline auto dispatch(Callback&& callback, event_kind tag_value, Args&&... args) {
    using kind = event_kind;
    switch (tag_value) {
        case kind::providing: return takatori::util::enum_tag_callback<kind::providing>(std::forward<Callback>(callback), std::forward<Args>(args)...);
        case kind::task_completed: return takatori::util::enum_tag_callback<kind::task_completed>(std::forward<Callback>(callback), std::forward<Args>(args)...);
        case kind::completion_instructed: return takatori::util::enum_tag_callback<kind::completion_instructed>(std::forward<Callback>(callback), std::forward<Args>(args)...);
    }
    std::abort();
}

// gcc7 confuses when enum_tag_t is used with different kind, e.g. field_type_kind and event_kind,
// and raises compile error.
// For workaround, define local enum_tag_t like struct and use it for event constructor to avoid type conflict.
// The rest of the event/dag_controller still uses original enum_tag_t and its dispatcher because
// translation units are separated and no compile error occurs.
// The problem doesn't happen on gcc 8 or newer. When moving to new gcc, remove these tags and recover
// use of original enum_tag_t.
template<auto Kind>
struct event_enum_tag_t {
    explicit event_enum_tag_t() = default;
};
template<auto Kind>
inline constexpr event_enum_tag_t<Kind> event_enum_tag {};

/*
 * @brief detailed information about external event
 */
class event {
public:
    using identity_type = model::step::identity_type;
    using port_index_type = model::step::port_index_type;
    event() = default;

    // FIXME use enum_tag_t instead of event_enum_tag_t when gcc is fixed (see comment for event_enum_tag_t)
    event(event_enum_tag_t<event_kind::task_completed>, identity_type step, model::task::identity_type task) :
        kind_(event_kind::task_completed), target_(step), task_(task)
    {}

    event(event_enum_tag_t<event_kind::providing>, identity_type step, port_kind pkind, port_index_type pindex) :
        kind_(event_kind::providing),
        target_(step),
        source_port_kind_(pkind),
        source_port_index_(pindex)
    {}

    [[nodiscard]] event_kind kind() const {
        return kind_;
    }

    [[nodiscard]] identity_type target() const {
        return target_;
    }

    [[nodiscard]] model::task::identity_type task() const {
        return task_;
    }

    [[nodiscard]] port_kind source_port_kind() const {
        return source_port_kind_;
    }

    [[nodiscard]] port_index_type source_port_index() const {
        return source_port_index_;
    }

private:
    event_kind kind_{};
    identity_type target_{};
    model::task::identity_type task_{};
    port_kind source_port_kind_{};
    port_index_type source_port_index_{};
};

static_assert(std::is_trivially_copyable_v<event>);

}

