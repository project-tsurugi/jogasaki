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

#include "task.h"
#include "task_scheduler_cfg.h"
#include <tateyama/impl/worker.h>
#include <tateyama/impl/queue.h>
#include <tateyama/impl/thread_control.h>
#include <tateyama/impl/task_ref.h>

namespace tateyama {

/**
 * @brief stealing based task scheduler
 */
class task_scheduler {
public:
    /**
     * @brief copy construct
     */
    task_scheduler(task_scheduler const&) = delete;

    /**
     * @brief move construct
     */
    task_scheduler(task_scheduler &&) = delete;

    /**
     * @brief copy assign
     */
    task_scheduler& operator=(task_scheduler const&) = delete;

    /**
     * @brief move assign
     */
    task_scheduler& operator=(task_scheduler &&) = delete;

    /**
     * @brief destruct task_scheduler
     */
    ~task_scheduler() = default;

    /**
     * @brief construct new object
     */
    explicit task_scheduler(task_scheduler_cfg cfg = {});

    /**
     * @brief schedule task
     * @param t the task to be scheduled. Caller must ensure the task is alive until the end of the execution.
     * @note this function is thread-safe. Multiple threads can safely call this function concurrently.
     */
    void schedule(task& t);

    /**
     * @brief schedule task on the specified worker
     * @param t the task to be scheduled. Caller must ensure the task is alive until the end of the execution.
     * @param index the preferred worker index for the task to execute. This puts the task on the queue that the specified
     * worker has, but doesn't ensure the task to be run by the worker if stealing happens.
     * @note this function is thread-safe. Multiple threads can safely call this function concurrently.
     */
    void schedule_at(task& t, std::size_t index);

    /**
     * @brief start the scheduler
     * @details start the scheduler
     * @note this function is *NOT* thread-safe. Only a thread must call this before using the scheduler.
     */
    void start();

    /**
     * @brief stop the scheduler
     * @details stop the scheduler and join the worker threads
     * @note this function is *NOT* thread-safe. Only a thread must call this when finishing using the scheduler.
     */
    void stop();

    /**
     * @brief accessor to the worker count
     * @return the number of worker (threads and queues)
     * @note this function is thread-safe. Multiple threads can safely call this function concurrently.
     */
    [[nodiscard]] std::size_t size() const noexcept;

private:
    task_scheduler_cfg cfg_{};
    std::size_t size_{};
    std::vector<impl::queue> queues_{};
    std::vector<impl::worker> workers_{};
    std::vector<impl::thread_control> threads_{};
    std::vector<impl::worker_stat> worker_stats_{};
    std::vector<context> contexts_{};
    std::atomic_size_t current_index_{};

    void prepare();
};

}

