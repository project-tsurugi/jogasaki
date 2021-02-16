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
#include "parallel_task_scheduler.h"

#include <jogasaki/executor/common/task.h>

namespace jogasaki::scheduler {

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

parallel_task_scheduler::parallel_task_scheduler(thread_params params) :
    threads_(params) {}

void parallel_task_scheduler::schedule_task(std::shared_ptr<model::task> const& task) {
    threads_.submit(proceeding_task_wrapper(task));
}

void parallel_task_scheduler::wait_for_progress() {
    // no-op - tasks are already running on threads
}

void parallel_task_scheduler::start() {
    threads_.start();
}

void parallel_task_scheduler::stop() {
    threads_.stop();
}

task_scheduler_kind parallel_task_scheduler::kind() const noexcept {
    return task_scheduler_kind::parallel;
}

proceeding_task_wrapper::proceeding_task_wrapper(std::weak_ptr<model::task> original) :
    original_(std::move(original))
{}

void proceeding_task_wrapper::operator()() {
    auto s = original_.lock();
    if (!s) return;
    while(s->operator()() == model::task_result::proceed) {}
}

}
