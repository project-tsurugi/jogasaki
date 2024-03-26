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

#include <cstddef>
#include <iosfwd>
#include <memory>

#include <jogasaki/scheduler/conditional_task.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/schedule_option.h>
#include <jogasaki/scheduler/serial_task_scheduler.h>
#include <jogasaki/scheduler/stealing_task_scheduler.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/scheduler/thread_params.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::scheduler {

/**
 * @brief task scheduler using multiple threads
 */
class cache_align hybrid_task_scheduler : public task_scheduler {
public:

    /**
     * @brief create new object
     */
    hybrid_task_scheduler() = default;

    ~hybrid_task_scheduler() override = default;
    hybrid_task_scheduler(hybrid_task_scheduler const& other) = delete;
    hybrid_task_scheduler& operator=(hybrid_task_scheduler const& other) = delete;
    hybrid_task_scheduler(hybrid_task_scheduler&& other) noexcept = delete;
    hybrid_task_scheduler& operator=(hybrid_task_scheduler&& other) noexcept = delete;

    /**
     * @brief create new object with thread params
     * @param params the parameters related with threading
     */
    explicit hybrid_task_scheduler(thread_params params);

    /**
     * @brief schedule the task
     * @param task the task to schedule
     * @pre scheduler is started
     */
    void do_schedule_task(flat_task&& t, schedule_option opt) override;

    /**
     * @brief schedule the conditional task
     * @param task the conditional task to schedule
     * @pre scheduler is started
     */
    void do_schedule_conditional_task(conditional_task&& t) override;

    /**
     * @brief wait for the scheduler to proceed
     * @details this is no-op for multi-thread scheduler
     */
    void wait_for_progress(std::size_t id) override;

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

    /**
     * @brief register the job context
     */
    void register_job(std::shared_ptr<job_context> ctx) override;

    /**
     * @brief declare the end of job and unregister it from the scheduler
     */
    void unregister_job(std::size_t job_id) override;

    /**
     * @brief print diagnostics
     */
    void print_diagnostic(std::ostream& os) override;

private:
    stealing_task_scheduler stealing_scheduler_{};
    serial_task_scheduler serial_scheduler_;
};

}



