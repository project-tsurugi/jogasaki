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

#include <jogasaki/model/task.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/utils/interference_size.h>
#include "details/thread_pool.h"
#include "task_scheduler.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

/*
 * @brief task scheduler using multiple threads
 */
class cache_align multi_thread_task_scheduler : public task_scheduler {
public:
    multi_thread_task_scheduler() = default;
    ~multi_thread_task_scheduler() override = default;
    multi_thread_task_scheduler(multi_thread_task_scheduler const& other) = delete;
    multi_thread_task_scheduler& operator=(multi_thread_task_scheduler const& other) = delete;
    multi_thread_task_scheduler(multi_thread_task_scheduler&& other) noexcept = delete;
    multi_thread_task_scheduler& operator=(multi_thread_task_scheduler&& other) noexcept = delete;
    explicit multi_thread_task_scheduler(thread_params params);

private:
    /**
     * @brief task wrapper to run the task continuously while task result is 'proceed'
     */
    class proceeding_task_wrapper {
    public:
        explicit proceeding_task_wrapper(std::weak_ptr<model::task> original);

        void operator()();
    private:
        std::weak_ptr<model::task> original_{};
    };

public:
    void schedule_task(std::shared_ptr<model::task> const& t) override;

    void wait_for_progress() override;

    void start() override;

    void stop() override;

    [[nodiscard]] task_scheduler_kind kind() const noexcept override;
private:
    std::unordered_map<model::task::identity_type, std::weak_ptr<model::task>> tasks_{};
    details::thread_pool threads_{};
};

}



