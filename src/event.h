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

#include <model/task.h>
#include <model/step.h>

namespace jogasaki {

/*
 * @brief external events handled in dag controller
 */
enum class event_kind {
    /*
     * @brief upstream step start sending data to downstream
     * Valid only when downstream step is not blocking exchange such as shuffle
     */
    upstream_providing,

    /*
     * @brief a task completed
     */
    task_completed,

    /*
     * @brief early completion has been requested
     */
    completion_instructed,
};

inline constexpr std::string_view to_string_view(event_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = event_kind;
    switch (value) {
        case kind::upstream_providing: return "upstream_providing"sv;
        case kind::task_completed: return "task_completed"sv;
        case kind::completion_instructed: return "completion_instructed"sv;
    }
    std::abort();
}

template<event_kind Kind>
using event_kind_tag_t = takatori::util::enum_tag_t<Kind>;

template<event_kind Kind>
inline constexpr event_kind_tag_t<Kind> event_kind_tag {};

template<class Callback, class... Args>
inline auto dispatch(Callback&& callback, event_kind tag_value, Args&&... args) {
    using kind = event_kind;
    switch (tag_value) {
        case kind::upstream_providing: return takatori::util::enum_tag_callback<kind::upstream_providing>(std::forward<Callback>(callback), std::forward<Args>(args)...);
        case kind::task_completed: return takatori::util::enum_tag_callback<kind::task_completed>(std::forward<Callback>(callback), std::forward<Args>(args)...);
        case kind::completion_instructed: return takatori::util::enum_tag_callback<kind::completion_instructed>(std::forward<Callback>(callback), std::forward<Args>(args)...);
    }
    std::abort();
}

/*
 * @brief detailed information about external event
 */
class event {
public:
    using identity_type = model::step::identity_type;
    using port_index_type = model::step::port_index_type;
    event() = default;
    ~event() = default;
    event(event&& other) noexcept = default;
    event& operator=(event&& other) noexcept = default;
    event(event_kind_tag_t<event_kind::task_completed> tag, identity_type step, model::task::identity_type task) : kind_(tag), target_(step), task_(task) {}
    event(event_kind_tag_t<event_kind::upstream_providing> tag, identity_type step, port_kind pkind, port_index_type pindex) : kind_(tag), target_(step), source_port_kind_(pkind), source_port_index_(pindex){}

    event_kind kind() {
        return kind_;
    }

    identity_type target() {
        return target_;
    }

    model::task::identity_type task() {
        return task_;
    }

    port_kind source_port_kind() {
        return source_port_kind_;
    }

    port_index_type source_port_index() {
        return source_port_index_;
    }

private:
    event_kind kind_{};
    identity_type target_{};
    model::task::identity_type task_{};
    port_kind source_port_kind_{};
    port_index_type source_port_index_{};
};

}

