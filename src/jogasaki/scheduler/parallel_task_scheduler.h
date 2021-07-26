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

#include <jogasaki/model/task.h>
#include "details/thread_pool.h"
#include "task_scheduler.h"
#include "thread_params.h"
#include <jogasaki/scheduler/flat_task.h>

namespace jogasaki::scheduler {

/**
 * @brief task scheduler using multiple threads
 */
class cache_align parallel_task_scheduler : public task_scheduler {
public:

    parallel_task_scheduler() = default;
    ~parallel_task_scheduler() override = default;
    parallel_task_scheduler(parallel_task_scheduler const& other) = delete;
    parallel_task_scheduler& operator=(parallel_task_scheduler const& other) = delete;
    parallel_task_scheduler(parallel_task_scheduler&& other) noexcept = delete;
    parallel_task_scheduler& operator=(parallel_task_scheduler&& other) noexcept = delete;
    explicit parallel_task_scheduler(thread_params params);

    /**
     * @brief schedule the task
     * @param task the task to schedule
     * @pre scheduler is started
     */
    void do_schedule_task(flat_task&& task) override;

    /**
     * @brief wait for the scheduler to proceed
     * @details this is no-op for multi-thread scheduler
     */
    void wait_for_progress(job_context& ctx) override;

    /**
     * @brief start the scheduler so that it's ready to accept request
     */
    void start() override;

    /**
     * @brief stop the scheduler joining all the running tasks and
     * canceling ones that are submitted but not yet executed
     */
    void stop() override;

    /**
     * @return kind of the task scheduler
     */
    [[nodiscard]] task_scheduler_kind kind() const noexcept override;

private:
    details::thread_pool threads_{};
};

}



