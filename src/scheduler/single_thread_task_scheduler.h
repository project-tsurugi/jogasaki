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

#include <unordered_set>

#include <model/task.h>
#include <channel.h>
#include "task_scheduler.h"

namespace jogasaki::scheduler {

/*
 * @brief task scheduler who is responsible for running task concurrently and efficiently
 * Current implementation is in single thread only and simulate concurrent execution by calling run() multiple times
 */
class single_thread_task_scheduler : public task_scheduler {
public:
    single_thread_task_scheduler() = default;
    ~single_thread_task_scheduler() override = default;
    single_thread_task_scheduler(single_thread_task_scheduler&& other) noexcept = delete;
    single_thread_task_scheduler& operator=(single_thread_task_scheduler&& other) noexcept = delete;

    void schedule_task(model::task* t) override {
        tasks_.emplace(t);
    }

    void run() override {
        for(auto it = tasks_.begin(); it != tasks_.end(); ) {
            if((*it)->operator()() == model::task_result::complete) {
                it = tasks_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void stop() override {
        // no-op
    }
private:
    std::unordered_set<model::task*> tasks_{};
};

}



