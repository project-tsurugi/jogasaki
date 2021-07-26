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

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <tateyama/context.h>
#include "thread_params.h"

namespace jogasaki::scheduler {

using takatori::util::maybe_shared_ptr;

/**
 * @brief field type kind
 */
enum class flat_task_kind : std::size_t {
    wrapped = 0,
    dag_events,
    bootstrap,
    teardown
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(flat_task_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = flat_task_kind;
    switch (value) {
        case kind::wrapped: return "wrapped"sv;
        case kind::dag_events: return "dag_events"sv;
        case kind::bootstrap: return "bootstrap"sv;
        case kind::teardown: return "teardown"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, flat_task_kind value) {
    return out << to_string_view(value);
}

template<auto Kind>
struct task_enum_tag_t {
    explicit task_enum_tag_t() = default;
};

template<auto Kind>
inline constexpr task_enum_tag_t<Kind> task_enum_tag {};

class flat_task {
public:
    using identity_type = std::size_t;
    static constexpr identity_type undefined_id = static_cast<identity_type>(-1);

    flat_task() = default;
    ~flat_task() = default;
    flat_task(flat_task const& other) = default;
    flat_task& operator=(flat_task const& other) = default;
    flat_task(flat_task&& other) noexcept = default;
    flat_task& operator=(flat_task&& other) noexcept = default;

    /**
     * @brief construct new object wrapping jogasaki task
     * @param origin
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::wrapped>,
        std::shared_ptr<model::task> origin,
        job_context* jctx
    ) noexcept :
        kind_(flat_task_kind::wrapped),
        origin_(std::move(origin)),
        job_context_(jctx)
    {}

    /**
     * @brief construct new object to run dag scheduler
     * @param jctx
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::dag_events>,
        job_context* jctx
    ) noexcept :
        kind_(flat_task_kind::dag_events),
        job_context_(jctx)
    {}

    /**
     * @brief construct new object to bootstrap dag scheduling
     * @param jctx
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::bootstrap>,
        model::graph& g,
        job_context* jctx
    ) noexcept :
        kind_(flat_task_kind::bootstrap),
        job_context_(jctx),
        graph_(std::addressof(g))
    {}

    /**
     * @brief construct new object to run dag scheduler
     * @param jctx
     */
    flat_task(
        task_enum_tag_t<flat_task_kind::teardown>,
        job_context* jctx
    ) noexcept :
        kind_(flat_task_kind::teardown),
        job_context_(jctx)
    {}

    /**
     * @brief getter for type kind
     */
    [[nodiscard]] constexpr flat_task_kind kind() const noexcept {
        return kind_;
    }

    [[nodiscard]] std::shared_ptr<model::task> const& origin() const noexcept {
        return origin_;
    }

    void operator()(tateyama::context& ctx);

    /**
     * @brief returns task id that uniquely identifies the task
     */
    [[nodiscard]] identity_type id() const;

    /**
     * @brief returns task id that uniquely identifies the task
     */
    [[nodiscard]] job_context* job() const {
        return job_context_;
    }

    friend std::ostream& operator<<(std::ostream& out, flat_task const& value) {
        return value.write_to(out);
    }
private:
    flat_task_kind kind_{};
    std::shared_ptr<model::task> origin_{};
    bool dag_scheduling_{false};
    bool bootstrap_{false};
    bool teardown_{false};
    request_context* request_context_{};
    job_context* job_context_{};
    model::graph* graph_{};

    void bootstrap();
    void dag_schedule();
    void teardown();

    std::ostream& write_to(std::ostream& out) const {
        using namespace std::string_view_literals;
        return out << "task[id="sv << std::to_string(static_cast<identity_type>(id())) << "]"sv;
    }

};

}



