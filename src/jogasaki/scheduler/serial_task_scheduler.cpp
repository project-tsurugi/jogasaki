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
#include "serial_task_scheduler.h"

#include <thread>
#include <unordered_set>

#include "task_scheduler.h"

namespace jogasaki::scheduler {

thread_local serial_task_scheduler::entity_type serial_task_scheduler::tasks_{};  //NOLINT

void serial_task_scheduler::do_schedule_task(
    flat_task&& task
) {
    tasks_.emplace_back(std::move(task));
}

void serial_task_scheduler::wait_for_progress(job_context&) {
    tateyama::api::task_scheduler::context ctx{std::hash<std::thread::id>{}(std::this_thread::get_id())};
    while(! tasks_.empty()) {
        auto& s = tasks_.front();
        s(ctx);
        tasks_.pop_front();
    }
}

void serial_task_scheduler::start() {
    // no-op
}

void serial_task_scheduler::stop() {
    tasks_.clear();
}

task_scheduler_kind serial_task_scheduler::kind() const noexcept {
    return task_scheduler_kind::serial;
}
}



