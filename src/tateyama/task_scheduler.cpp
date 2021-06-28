/*
 * Copyright 2018-2021 tsurugi project.
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
#include "task_scheduler.h"

#include <tateyama/impl/task_ref.h>

namespace tateyama {

std::size_t increment(std::atomic_size_t& index, std::size_t mod) {
    auto ret = index++;
    return ret % mod;
}

void task_scheduler::schedule(std::shared_ptr<task> t) {
    auto& q = queues_[increment(current_index_, size_)];
    q.push(impl::task_ref{t});
}

void task_scheduler::schedule_at(std::shared_ptr<task> t, std::size_t index) {
    BOOST_ASSERT(index < size_); //NOLINT
    auto& q = queues_[index];
    q.push(impl::task_ref{t});
}

task_scheduler::task_scheduler(task_scheduler_cfg cfg) :
    cfg_(cfg),
    size_(cfg_.thread_count())
{
    prepare();
}

void task_scheduler::start() {
    for(auto&& t : threads_) {
        t.activate();
    }
}

void task_scheduler::stop() {
    for(auto&& q : queues_) {
        q.deactivate();
    }
    for(auto&& t : threads_) {
        t.join();
    }
}

std::size_t task_scheduler::size() const noexcept {
    return size_;
}

void task_scheduler::prepare() {
    auto sz = cfg_.thread_count();
    queues_.resize(sz);
    worker_stats_.resize(sz);
    contexts_.reserve(sz);
    workers_.reserve(sz);
    threads_.reserve(sz);
    for(std::size_t i = 0; i < sz; ++i) {
        auto& ctx = contexts_.emplace_back(i);
        auto& worker = workers_.emplace_back(queues_, worker_stats_[i], std::addressof(cfg_));
        threads_.emplace_back(i, std::addressof(cfg_), worker, ctx);
    }
}

}


