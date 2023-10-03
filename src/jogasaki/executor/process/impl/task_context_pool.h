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
#pragma once

#include <memory>
#include <vector>

#include <tbb/concurrent_queue.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief task context pool
 * @details a thread-safe task context container
 */
class cache_align task_context_pool {
public:
    /**
     * @brief create new empty instance
     */
    task_context_pool() = default;

    /**
     * @brief create new empty instance
     */
    explicit task_context_pool(std::vector<std::shared_ptr<abstract::task_context>> contexts);

    /**
     * @brief add new task context
     * @details this function can be called from multiple threads
     * @param context the context to add
     */
    void push(std::shared_ptr<abstract::task_context> context);

    /**
     * @brief fetch the task context on top
     * @details this function can be called from multiple threads
     * @return the fetched context
     */
    std::shared_ptr<abstract::task_context> pop();

private:
    tbb::concurrent_bounded_queue<std::shared_ptr<abstract::task_context>> contexts_{};
};

}


