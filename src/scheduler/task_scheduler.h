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

#include <model/task.h>

namespace jogasaki::scheduler {

enum class task_scheduler_kind : std::int32_t {
    single_thread = 0,
    multi_thread,
};

/*
 * @brief task scheduler to run tasks efficiently
 */
class task_scheduler {
public:
    task_scheduler() = default;
    virtual ~task_scheduler() = default;
    task_scheduler(task_scheduler const& other) = delete;
    task_scheduler& operator=(task_scheduler const& other) = delete;
    task_scheduler(task_scheduler&& other) noexcept = delete;
    task_scheduler& operator=(task_scheduler&& other) noexcept = delete;

    virtual void schedule_task(std::weak_ptr<model::task> t) = 0;

    virtual void proceed() = 0;

    virtual void start() = 0;

    virtual void stop() = 0;

    virtual task_scheduler_kind kind() const noexcept = 0;
};

}



