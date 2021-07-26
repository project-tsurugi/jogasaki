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
    explicit flat_task(
        std::shared_ptr<model::task> origin,
        job_context* jctx
    ) noexcept :
        origin_(std::move(origin)),
        job_context_(jctx)
    {}

    /**
     * @brief construct new object to run dag scheduler
     * @param jctx
     */
    explicit flat_task(
        job_context* jctx
    ) noexcept :
        dag_scheduling_(true),
        job_context_(jctx)
    {}

    /**
     * @brief construct new object to bootstrap dag scheduling
     * @param jctx
     */
    flat_task(
        model::graph& g,
        job_context* jctx
    ) noexcept :
        bootstrap_(true),
        job_context_(jctx),
        graph_(std::addressof(g))
    {}

    /**
     * @brief construct new object to run dag scheduler
     * @param jctx
     */
    explicit flat_task(
        bool teardown,  //TODO
        job_context* jctx
    ) noexcept :
        teardown_(true),
        job_context_(jctx)
    {
        (void) teardown;

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

    std::ostream& write_to(std::ostream& out) const {
        using namespace std::string_view_literals;
        return out << "task[id="sv << std::to_string(static_cast<identity_type>(id())) << "]"sv;
    }

    friend std::ostream& operator<<(std::ostream& out, flat_task const& value) {
        return value.write_to(out);
    }
private:
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
};

}



