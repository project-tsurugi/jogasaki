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

#include <cstddef>
#include <ostream>

namespace jogasaki::model {

enum class task_kind : std::size_t {
    main = 0,
    pre,
};

/**
 * @brief response codes to the scheduler in order to inform how to treat the finished task
 */
enum class task_result : std::size_t {
    /**
     * @brief proceed to continue execution, expecting scheduler to schedule the task soon
     */
    proceed,

    /**
     * @brief complete the task and remove it from the schedule
     */
    complete,

    /**
     * @brief sleep and detach thread from this task, the task needs wake-up to resume
     * @attention this is future functionality and not yet supported
     */
    sleep,

    /**
     * @brief yield to other tasks, if any, by putting this task at the last of run queue
     * @attention this is future functionality and not yet supported
     */
    yield,
};

class task {
public:
    using identity_type = std::size_t;

    /**
     * @brief returns task id that uniquely identifies the task
     */
    [[nodiscard]] virtual identity_type id() const = 0;

    /**
     * @brief creates a new instance.
     */
    task() = default;

    /**
     * @brief destroys this object.
     */
    virtual ~task() = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    task(task const& other) = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    task& operator=(task const& other) = default;

    /**
     * @brief creates a new instance.
     * @param other the source object
     */
    task(task&& other) noexcept = default;

    /**
     * @brief assigns the given object.
     * @param other the source object
     * @return this
     */
    task& operator=(task&& other) noexcept = default;

    /**
     * @brief task body
     * @return task_result to instruct scheduler
     */
    [[nodiscard]] virtual task_result operator()() = 0;

    /**
     * @brief accessor to I/O operation property of the task
     * @return whether the task contains transactional I/O operations that requires special handling in scheduling
     */
    [[nodiscard]] virtual bool has_transactional_io() = 0;

protected:
    virtual std::ostream& write_to(std::ostream& out) const = 0;

public:
    friend std::ostream& operator<<(std::ostream& out, task const& value) {
        return value.write_to(out);
    }
};

}
