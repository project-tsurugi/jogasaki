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

#include <deque>
#include <memory>

#include <jogasaki/model/task.h>
#include <jogasaki/utils/interference_size.h>
#include "task_scheduler.h"

namespace jogasaki::scheduler {

/**
 * @brief task scheduler using only single thread
 */
class cache_align serial_task_scheduler : public task_scheduler {
public:
    using entity_type = std::deque<flat_task>;

    /**
     * @brief schedule the task
     * @param task the task to schedule
     * @pre scheduler is started
     */
    void schedule_task(flat_task&& task) override;

    /**
     * @brief wait for the scheduler to proceed
     */
    void wait_for_progress(job_context& ctx) override;

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

private:
    static thread_local entity_type tasks_;
};

}



