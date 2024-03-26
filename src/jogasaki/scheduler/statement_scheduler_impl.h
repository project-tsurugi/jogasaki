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

#include <memory>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/configuration.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::scheduler {

/**
 * @brief impl of the statement_scheduler
 * @details the implementation is accessible from the code in the scheduler package scope
 */
class cache_align statement_scheduler::impl {
public:
    /**
     * @brief create new object
     */
    impl(std::shared_ptr<configuration> cfg, task_scheduler& scheduler);

    /**
     * @brief create new object
     */
    explicit impl(std::shared_ptr<configuration> cfg);

    /**
     * @brief create new object from dag_controller
     */
    explicit impl(maybe_shared_ptr<dag_controller> controller);

    /**
     * @brief schedule the statement to run
     * @details this is deprecated and left for testing purpose. Scheduling should be done with task_scheduler.
     */
    void schedule(
        model::statement const& s,
        request_context& context
    );

    /**
     * @brief accessor to the dag controller
     */
    [[nodiscard]] dag_controller& controller() noexcept;

    /**
     * @brief accessor to the task scheduler
     */
    task_scheduler& get_task_scheduler() noexcept;

    /**
     * @brief function to get the impl
     */
    static statement_scheduler::impl& get_impl(statement_scheduler& arg);

private:

    maybe_shared_ptr<dag_controller> dag_controller_{};
    std::shared_ptr<configuration> cfg_{};
};

} // namespace
