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

#include <jogasaki/callback.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/abstract/processor.h>

namespace jogasaki::executor::process::abstract {

/**
 * @brief process executor interface
 * @details process executor is responsible to choose task context, assign and execute the processor in order to
 * complete the work assigned to a processor task.
 * Optionally callbacks can be set before/after run() add customization point (for perf. testing purpose)
 * The object should be thread safe.
 */
class process_executor {
public:
    using status = abstract::status;

    process_executor() = default;

    process_executor(process_executor const& other) = delete;
    process_executor& operator=(process_executor const& other) = delete;
    process_executor(process_executor&& other) noexcept = delete;
    process_executor& operator=(process_executor&& other) noexcept = delete;

    virtual ~process_executor() = default;

    [[nodiscard]] virtual status run() = 0;

    /**
     * @brief sets callback before creating tasks
     * @param arg the callback
     */
    void will_run(std::shared_ptr<callback_type> arg) {
        will_run_ = std::move(arg);
    }

    /**
     * @brief gets callback before creating tasks
     * @return the callback
     */
    [[nodiscard]] std::shared_ptr<callback_type> const& will_run() const noexcept {
        return will_run_;
    }

    /**
     * @brief sets callback after creating tasks
     * @param arg the callback
     */
    void did_run(std::shared_ptr<callback_type> arg) {
        did_run_ = std::move(arg);
    }

    /**
     * @brief gets callback after creating tasks
     * @return the callback
     */
    [[nodiscard]] std::shared_ptr<callback_type> const& did_run() const noexcept {
        return did_run_;
    }

private:
    std::shared_ptr<callback_type> did_assign_task_{};
    std::shared_ptr<callback_type> will_run_{};
    std::shared_ptr<callback_type> did_run_{};
};

/**
 * @brief process executor factory to instantiate the process executor
 */
using process_executor_factory = std::function<std::shared_ptr<process_executor>(
    std::shared_ptr<abstract::processor> processor,
    std::vector<std::shared_ptr<abstract::task_context>>
)>;

}


