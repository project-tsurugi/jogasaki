/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <deque>
#include <iosfwd>
#include <memory>
#include <tbb/concurrent_hash_map.h>

#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/conditional_task.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/schedule_option.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::scheduler {

/**
 * @brief serial task scheduler
 * @details this task scheduler accumulate the submitted tasks and run them on single thread,
 * which calls wait_for_progress()
 */
class cache_align serial_task_scheduler : public task_scheduler {
public:
    using entity_type = std::deque<flat_task>;

    using conditional_entity_type = std::deque<conditional_task>;
    /**
     * @brief schedule the task
     * @param task the task to schedule
     * @param opt schedule option
     * @pre scheduler is started
     */
    void do_schedule_task(flat_task&& task, schedule_option opt) override;

    /**
     * @brief schedule the task
     * @param task the task to schedule
     * @pre scheduler is started
     */
    void do_schedule_conditional_task(conditional_task&& task) override;

    /**
     * @brief wait for the scheduler to proceed
     */
    void wait_for_progress(std::size_t id) override;

    /**
     * @brief start the scheduler
     */
    void start() override;

    /**
     * @brief stop the scheduler
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

    static thread_local entity_type tasks_;  //NOLINT
    static thread_local conditional_entity_type conditional_tasks_;  //NOLINT

    // a job can be started / finished on different threads
    tbb::concurrent_hash_map<std::size_t, std::shared_ptr<job_context>> job_contexts_{};
};

}



