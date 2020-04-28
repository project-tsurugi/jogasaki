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

#include <cstddef>
#include <ostream>

namespace jogasaki::model {

enum class task_kind : std::size_t {
    main = 0,
    pre,
};

enum class task_result : std::size_t {
    /**
     * @brief proceed to process next input
     */
    proceed,

    /**
     * @brief complete the task and clean up all resources
     */
    complete,

    /**
     * @brief sleep and detach thread from this task
     */
    sleep,

    /**
     * @brief yield to other tasks, if any, by putting this task at the last of run queue
     */
    yield,
};

class task {
public:
    using identity_type = std::size_t;

    [[nodiscard]] virtual identity_type id() const = 0;
    task() = default;
    virtual ~task() = default;
    task(task&& other) noexcept = default;
    task& operator=(task&& other) noexcept = default;

    /**
     * @brief task body
     * @return true when there is more work to do
     * @return false to finish running
     */
    virtual task_result operator()() = 0;

protected:
    virtual std::ostream& write_to(std::ostream& out) const = 0;

public:
    friend std::ostream& operator<<(std::ostream& out, task const& value) {
        return value.write_to(out);
    }
};

}
