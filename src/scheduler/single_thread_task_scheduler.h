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
#include <memory>

#include <model/task.h>
#include "task_scheduler.h"

namespace jogasaki::scheduler {

/*
 * @brief task scheduler who is responsible for running task concurrently and efficiently
 * Current implementation is in single thread only and simulate concurrent execution by calling run() multiple times
 */
class single_thread_task_scheduler : public task_scheduler {
public:
    void schedule_task(std::weak_ptr<model::task> t) override {
        auto s = t.lock();
        if (s) {
            auto id = s->id();
            tasks_.emplace(id, std::move(t));
        }
    }

    void proceed() override {
        for(auto it = tasks_.begin(); it != tasks_.end(); ) {
            auto s = it->second.lock();
            if (s == nullptr || s->operator()() == model::task_result::complete) {
                it = tasks_.erase(it);
            } else {
                ++it;
            }
        }
    }

    void start() override {
        // no-op
    }

    void stop() override {
        tasks_.clear();
    }

    [[nodiscard]] task_scheduler_kind kind() const noexcept override {
        return task_scheduler_kind::single_thread;
    }
private:
    std::unordered_map<model::task::identity_type, std::weak_ptr<model::task>> tasks_{};
};

}



