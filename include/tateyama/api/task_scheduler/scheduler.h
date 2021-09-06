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
#pragma once

#include "task_scheduler_cfg.h"
#include <tateyama/task_scheduler/worker.h>
#include <tateyama/task_scheduler/queue.h>
#include <tateyama/task_scheduler/thread_control.h>
#include <tateyama/task_scheduler/cache_align.h>

namespace tateyama::api::task_scheduler {

/**
 * @brief stealing based task scheduler
 * @tparam T the task type. See comments for `task`.
 */
template <class T>
class cache_align scheduler {
public:

    /**
     * @brief task type scheduled by this
     * @details the task object must be default constructible, move constructible, and move assignable.
     * Interaction with local task queues are done in move semantics.
     */
    using task = T;

    /**
     * @brief queue used by this scheduler implementation
     */
    using queue = tateyama::task_scheduler::basic_queue<task>;

    /**
     * @brief worker used by this scheduler implementation
     */
    using worker = tateyama::task_scheduler::worker<task>;

    /**
     * @brief copy construct
     */
    scheduler(scheduler const&) = delete;

    /**
     * @brief move construct
     */
    scheduler(scheduler &&) = delete;

    /**
     * @brief copy assign
     */
    scheduler& operator=(scheduler const&) = delete;

    /**
     * @brief move assign
     */
    scheduler& operator=(scheduler &&) = delete;

    /**
     * @brief destruct scheduler
     */
    ~scheduler() = default;

    /**
     * @brief construct new object
     */
    explicit scheduler(task_scheduler_cfg cfg = {}) :
        cfg_(cfg),
        size_(cfg_.thread_count())
    {
        prepare();
    }

    /**
     * @brief schedule task
     * @param t the task to be scheduled.
     * @note this function is thread-safe. Multiple threads can safely call this function concurrently.
     */
    void schedule(task&& t) {
        std::size_t index{};
        if (cfg_.round_robbin()) {
            index = increment(current_index_, size_);
        } else {
            constexpr static auto undefined = static_cast<std::size_t>(-1);
            thread_local std::size_t index_for_this_thread = undefined;
            if (index_for_this_thread == undefined) {
                index = increment(current_index_, size_);
                index_for_this_thread = index;
            } else {
                index = index_for_this_thread;
            }
        }
        schedule_at(std::move(t), index);
    }

    /**
     * @brief schedule task on the specified worker
     * @param t the task to be scheduled.
     * @param index the preferred worker index for the task to execute. This puts the task on the queue that the specified
     * worker has, but doesn't ensure the task to be run by the worker if stealing happens.
     * @note this function is thread-safe. Multiple threads can safely call this function concurrently.
     */
    void schedule_at(task&& t, std::size_t index) {
        BOOST_ASSERT(index < size_); //NOLINT
        if (! started_) {
            auto& s = initial_tasks_[index];
            s.emplace_back(std::move(t));
            return;
        }
        auto& q = queues_[index];
        q.push(std::move(t));
    }

    /**
     * @brief start the scheduler
     * @details start the scheduler
     * @note this function is *NOT* thread-safe. Only a thread must call this before using the scheduler.
     */
    void start() {
        for(auto&& t : threads_) {
            t.activate();
        }
        started_ = true;
    }

    /**
     * @brief stop the scheduler
     * @details stop the scheduler and join the worker threads
     * @note this function is *NOT* thread-safe. Only a thread must call this when finishing using the scheduler.
     */
    void stop() {
        for(auto&& q : queues_) {
            q.deactivate();
        }
        for(auto&& t : threads_) {
            t.join();
        }
        started_ = false;
    }

    /**
     * @brief accessor to the worker count
     * @return the number of worker (threads and queues)
     * @note this function is thread-safe. Multiple threads can safely call this function concurrently.
     */
    [[nodiscard]] std::size_t size() const noexcept {
        return size_;
    }
    /**
     * @brief accessor to the worker statistics
     * @note this function is thread-safe. Multiple threads can safely call this function concurrently.
     */
    [[nodiscard]] std::vector<tateyama::task_scheduler::worker_stat> const& worker_stats() const noexcept {
        return worker_stats_;
    }

    /**
     * @brief accessor to the local queue for testing purpose
     * @note this function is thread-safe. Multiple threads can safely call this function concurrently.
     */
    [[nodiscard]] std::vector<queue> const& queues() const noexcept {
        return queues_;
    }

private:
    task_scheduler_cfg cfg_{};
    std::size_t size_{};
    std::vector<queue> queues_{};
    std::vector<worker> workers_{};
    std::vector<tateyama::task_scheduler::thread_control> threads_{};
    std::vector<tateyama::task_scheduler::worker_stat> worker_stats_{};
    std::vector<context> contexts_{};
    std::atomic_size_t current_index_{};
    std::vector<std::vector<task>> initial_tasks_{};
    std::atomic_bool started_{false};

    void prepare() {
        auto sz = cfg_.thread_count();
        queues_.resize(sz);
        worker_stats_.resize(sz);
        initial_tasks_.resize(sz);
        contexts_.reserve(sz);
        workers_.reserve(sz);
        threads_.reserve(sz);
        for(std::size_t i = 0; i < sz; ++i) {
            auto& ctx = contexts_.emplace_back(i);
            auto& worker = workers_.emplace_back(queues_, initial_tasks_, worker_stats_[i], std::addressof(cfg_));
            threads_.emplace_back(i, std::addressof(cfg_), worker, ctx);
        }
    }

    std::size_t increment(std::atomic_size_t& index, std::size_t mod) {
        auto ret = index++;
        return ret % mod;
    }

    static_assert(std::is_default_constructible_v<task>);
    static_assert(std::is_move_constructible_v<task>);
    static_assert(std::is_move_assignable_v<task>);
};

}

