/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include <takatori/util/exception.h>

#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/task_scheduler.h>

namespace jogasaki::scheduler {

using takatori::util::throw_exception;

thread_local serial_task_scheduler::entity_type serial_task_scheduler::tasks_{};  //NOLINT
thread_local serial_task_scheduler::conditional_entity_type serial_task_scheduler::conditional_tasks_;  //NOLINT

void serial_task_scheduler::do_schedule_task(
    flat_task&& task,
    schedule_option opt
) {
    (void) opt;
    tasks_.emplace_back(std::move(task));
}

void serial_task_scheduler::do_schedule_conditional_task(
    conditional_task&& task
) {
    conditional_tasks_.emplace_back(std::move(task));
}

void serial_task_scheduler::wait_for_progress(std::size_t) {
    tateyama::task_scheduler::context ctx{std::hash<std::thread::id>{}(std::this_thread::get_id())};
    while(true) {
        if(! tasks_.empty()) {
            auto s = std::move(tasks_.front());
            tasks_.pop_front();
            s(ctx);
            continue;
        }
        if(! conditional_tasks_.empty()) {
            auto s = std::move(conditional_tasks_.front());
            conditional_tasks_.pop_front();
            if(s.check()) {
                s();
                continue;
            }
            conditional_tasks_.push_back(std::move(s));
            continue;
        }
        break;
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

void serial_task_scheduler::register_job(std::shared_ptr<job_context> ctx) {
    auto cid = ctx->id();
    if(! job_contexts_.emplace(cid, std::move(ctx))) {
        throw_exception(std::logic_error{""});
    }
}

void serial_task_scheduler::unregister_job(std::size_t job_id) {
    if(! job_contexts_.erase(job_id)) {
        throw_exception(std::logic_error{""});
    }
}

void serial_task_scheduler::print_diagnostic(std::ostream &os) {
    (void) os;
    // no-op
}
}



