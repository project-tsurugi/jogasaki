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

#include <glog/logging.h>
#include <takatori/util/fail.h>

#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/flat_task.h>

namespace jogasaki::scheduler {

using takatori::util::fail;

class job_context;

enum class task_scheduler_kind : std::int32_t {
    serial = 0,
    parallel,
    stealing,
};

/**
 * @brief task scheduler to run tasks efficiently
 */
class task_scheduler {
public:
    /**
     * @brief create new object
     */
    task_scheduler() = default;

    /**
     * @brief destroy instance
     */
    virtual ~task_scheduler() = default;

    task_scheduler(task_scheduler const& other) = delete;
    task_scheduler& operator=(task_scheduler const& other) = delete;
    task_scheduler(task_scheduler&& other) noexcept = delete;
    task_scheduler& operator=(task_scheduler&& other) noexcept = delete;

    /**
     * @brief schedule the task, the subclass needs to implement
     * @param t the task to schedule
     */
    virtual void do_schedule_task(flat_task&& t) = 0;

    /**
     * @brief schedule the task
     * @param t the task to schedule
     * @pre scheduler is started
     */
    void schedule_task(flat_task&& t) {
        if (t.job()->completing() && t.kind() != flat_task_kind::teardown) {
            // if the job is already submitted teardown task, the number of task should never grow.
            // teardown task is only the exception since it can reschedule itself.
            LOG(ERROR) << "task submitted too late : " << t.kind();
            return;
        }
        ++t.job()->task_count();
        do_schedule_task(std::move(t));
    }

    /**
     * @brief wait for the scheduler to proceed
     * @param ctx the context of the job whose completion is waited
     * @details the caller blocks until the job completes
     */
    virtual void wait_for_progress(job_context& ctx) = 0;

    /**
     * @brief start the scheduler so that it's ready to accept request
     */
    virtual void start() = 0;

    /**
     * @brief stop the scheduler joining all the running tasks and
     * canceling ones that are submitted but not yet executed
     */
    virtual void stop() = 0;

    /**
     * @return kind of the task scheduler
     */
    [[nodiscard]] virtual task_scheduler_kind kind() const noexcept = 0;
};

}



