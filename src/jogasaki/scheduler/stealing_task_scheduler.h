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
#include <tbb/concurrent_hash_map.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <tateyama/api/task_scheduler/scheduler.h>
#include <tateyama/api/task_scheduler/context.h>
#include <tateyama/api/task_scheduler/task_scheduler_cfg.h>
#include "task_scheduler.h"
#include "thread_params.h"
#include "flat_task.h"

namespace jogasaki::scheduler {

using takatori::util::maybe_shared_ptr;

/**
 * @brief task scheduler using multiple threads
 */
class cache_align stealing_task_scheduler : public task_scheduler {
public:

    /**
     * @brief create new object
     */
    stealing_task_scheduler() = default;

    ~stealing_task_scheduler() override = default;
    stealing_task_scheduler(stealing_task_scheduler const& other) = delete;
    stealing_task_scheduler& operator=(stealing_task_scheduler const& other) = delete;
    stealing_task_scheduler(stealing_task_scheduler&& other) noexcept = delete;
    stealing_task_scheduler& operator=(stealing_task_scheduler&& other) noexcept = delete;

    /**
     * @brief create new object with thread params
     * @param params the parameters related with threading
     */
    explicit stealing_task_scheduler(thread_params params);

    /**
     * @brief schedule the task
     * @param task the task to schedule
     * @pre scheduler is started
     */
    void do_schedule_task(flat_task&& t) override;

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

    /**
     * @brief register the job context
     */
    void register_job(std::shared_ptr<job_context> ctx) override;

    /**
     * @brief declare the end of job and unregister it from the scheduler
     */
    void unregister_job(std::size_t job_id) override;

private:
    tateyama::api::task_scheduler::task_scheduler_cfg scheduler_cfg_{};
    tateyama::api::task_scheduler::scheduler<flat_task> scheduler_;
    tbb::concurrent_hash_map<std::size_t, std::shared_ptr<job_context>> job_contexts_{};

    tateyama::api::task_scheduler::task_scheduler_cfg create_scheduler_cfg(thread_params params);
};

}



