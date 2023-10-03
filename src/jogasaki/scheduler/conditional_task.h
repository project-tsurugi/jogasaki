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

#include <tateyama/task_scheduler/basic_conditional_task.h>

#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/hex.h>
#include <jogasaki/common.h>
#include <jogasaki/scheduler/job_context.h>
#include "thread_params.h"

namespace jogasaki::scheduler {

/**
 * @brief conditional task object
 */
class cache_align conditional_task {
public:
    /**
     * @brief condition type
     */
    using condition_type = std::function<bool(void)>;

    /**
     * @brief body type
     */
    using body_type = std::function<void(void)>;

    /**
     * @brief create new object
     */
    conditional_task() = default;

    ~conditional_task() = default;
    conditional_task(conditional_task const& other) = default;
    conditional_task& operator=(conditional_task const& other) = default;
    conditional_task(conditional_task&& other) noexcept = default;
    conditional_task& operator=(conditional_task&& other) noexcept = default;

    /**
     * @brief construct new object wrapping jogasaki task
     * @param rctx the request context where the task belongs
     * @param condition the check logic to be run to determine if task body is ready to execute
     * @param body the task body to be executed if the condition is met. The body is expected to be non-blocking and
     * light-weight such as scheduling another task.
     */
    conditional_task(
        request_context* rctx,
        condition_type condition,
        body_type body
    ) noexcept :
        req_context_(rctx),
        condition_(std::move(condition)),
        body_(std::move(body))
    {}

    /**
     * @brief execute the task
     */
    bool check() {
        return condition_();
    }

    /**
     * @brief execute the task
     */
    void operator()() {
        body_();
        --job()->task_count();
    }

    /**
     * @brief accessor to the job context that the task belongs to.
     */
    [[nodiscard]] job_context* job() const {
        return req_context_->job().get();
    }

    /**
     * @brief dump the text representation of the value to output stream
     * @param out the target output stream
     * @param value the value to be output
     */
    friend std::ostream& operator<<(std::ostream& out, conditional_task const& value) {
        return value.write_to(out);
    }

    /**
     * @brief accessor to the job context that the task belongs to.
     */
    [[nodiscard]] request_context* req_context() const noexcept {
        return req_context_.get();
    }

private:
    maybe_shared_ptr<request_context> req_context_{};
    condition_type condition_{};
    body_type body_{};

    std::ostream& write_to(std::ostream& out) const {
        using namespace std::string_view_literals;
        return out << "conditional_task"sv;
    }
};

inline void print_task_diagnostic(conditional_task const& t, std::ostream& os) {
    (void) t;
    (void) os;
}

}
