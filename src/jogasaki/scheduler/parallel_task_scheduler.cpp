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
#include <jogasaki/scheduler/job_context.h>

namespace jogasaki::scheduler {

/**
 * @brief task wrapper to run the task continuously while task result is 'proceed'
 */
class proceeding_task_wrapper {
public:
    proceeding_task_wrapper() = default;
    ~proceeding_task_wrapper() = default;
    proceeding_task_wrapper(proceeding_task_wrapper const& other) = default;
    proceeding_task_wrapper& operator=(proceeding_task_wrapper const& other) = default;
    proceeding_task_wrapper(proceeding_task_wrapper&& other) noexcept = default;
    proceeding_task_wrapper& operator=(proceeding_task_wrapper&& other) noexcept = default;
    explicit proceeding_task_wrapper(flat_task&& original);

    void operator()();

private:
    flat_task original_{};
};

parallel_task_scheduler::parallel_task_scheduler(thread_params params) :
    threads_(params)
{}

void parallel_task_scheduler::do_schedule_task(flat_task&& task) {
    threads_.submit(proceeding_task_wrapper(std::move(task)));
}

void parallel_task_scheduler::wait_for_progress(job_context& ctx) {
    DVLOG(1) << "wait_for_progress";
    ctx.completion_latch().wait();
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

proceeding_task_wrapper::proceeding_task_wrapper(flat_task&& original) :
    original_(std::move(original))
{}

void proceeding_task_wrapper::operator()() {
    tateyama::api::task_scheduler::context ctx{std::hash<std::thread::id>{}(std::this_thread::get_id())};
    original_(ctx);
}

}
