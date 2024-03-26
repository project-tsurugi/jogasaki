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
#include "job_context.h"

#include <utility>

#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/utils/latch.h>

namespace jogasaki::scheduler {

utils::latch& job_context::completion_latch() noexcept {
    return completion_latch_;
}

std::atomic_bool& job_context::completing() noexcept {
    return completing_;
}

std::atomic_size_t& job_context::task_count() noexcept {
    return job_tasks_;
}

std::atomic_size_t& job_context::preferred_worker_index() noexcept {
    return preferred_worker_index_;
}

void job_context::reset() noexcept {
    completion_latch_.reset();
    completing_.store(false);
    job_tasks_.store(0);
    preferred_worker_index_.store(undefined_index);
}

void job_context::callback(job_context::job_completion_callback callback) noexcept {
    callback_ = std::move(callback);
}

job_context::job_completion_callback& job_context::callback() noexcept {
    return callback_;
}

std::size_t job_context::id() const noexcept {
    return id_;
}

std::atomic_bool &job_context::started() noexcept {
    return started_;
}

void job_context::request(std::shared_ptr<request_detail> arg) noexcept {
    if(arg) {
        id_ = arg->id();
    }
    request_detail_ = std::move(arg);
}

std::shared_ptr<request_detail> const &job_context::request() const noexcept {
    return request_detail_;
}

void job_context::completion_readiness(readiness_provider checker) noexcept {
    readiness_provider_ = std::move(checker);
}

job_context::readiness_provider& job_context::completion_readiness() noexcept {
    return readiness_provider_;
}
}

