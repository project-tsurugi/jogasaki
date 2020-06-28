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
#pragma once

#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/executor/process/abstract/work_context.h>
#include <jogasaki/executor/process/abstract/scan_info.h>

namespace jogasaki::executor::process::impl {

/**
 * @brief task context pool
 * @details a context pool represents tasks that are possibly run by a processor.
 * This object is thread-safe, so can be used from process::task instances running on different threads.
 */
class task_context_pool {
public:
    /**
     * @brief create new empty instance
     */
    task_context_pool() = default;

    void push(std::shared_ptr<abstract::task_context> context) {
        contexts_.emplace(std::move(context));
    }

    std::shared_ptr<abstract::task_context> pop() {
        std::shared_ptr<abstract::task_context> context{};
        contexts_.pop(context);
        return context;
    }

private:
    tbb::concurrent_bounded_queue<std::shared_ptr<abstract::task_context>> contexts_{};
};

}


