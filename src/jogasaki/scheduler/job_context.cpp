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
#include "job_context.h"

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/utils/latch.h>
#include <jogasaki/scheduler/statement_scheduler.h>

namespace jogasaki::scheduler {

using takatori::util::maybe_shared_ptr;

void job_context::dag_scheduler(maybe_shared_ptr<scheduler::statement_scheduler> arg) noexcept {
    dag_scheduler_ = std::move(arg);
}

maybe_shared_ptr<scheduler::statement_scheduler> const& job_context::dag_scheduler() const noexcept {
    return dag_scheduler_;
}

utils::latch& job_context::completion_latch() noexcept {
    return completion_latch_;
}

std::atomic_bool& job_context::completing() noexcept {
    return completing_;
}

std::atomic_size_t& job_context::task_count() noexcept {
    return job_tasks_;
}

std::atomic_size_t& job_context::index() noexcept {
    return index_;
}

void job_context::reset() noexcept {
    completion_latch_.reset();
    completing_.store(false);
    job_tasks_.store(0);
    index_.store(undefined_index);
}

job_context::job_context(
    maybe_shared_ptr<scheduler::statement_scheduler> statement,
    std::size_t invoker_thread_cpu_id
) noexcept:
    dag_scheduler_(std::move(statement)),
    invoker_thread_cpu_id_(invoker_thread_cpu_id)
{}

}

