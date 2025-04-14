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
#include <cstdint>
#include <ostream>

namespace jogasaki::model {

enum class task_kind : std::size_t {
    main = 0,
    pre,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(task_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = task_kind;
    switch (value) {
        case kind::main: return "main"sv;
        case kind::pre: return "pre"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, task_kind value) {
    return out << to_string_view(value);
}

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
     * @brief complete the task, remove it from the schedule and teardown the job
     */
    complete_and_teardown,

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

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(task_result value) noexcept {
    using namespace std::string_view_literals;
    using kind = task_result;
    switch (value) {
        case kind::proceed: return "proceed"sv;
        case kind::complete: return "complete"sv;
        case kind::complete_and_teardown: return "complete_and_teardown"sv;
        case kind::sleep: return "sleep"sv;
        case kind::yield: return "yield"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, task_result value) {
    return out << to_string_view(value);
}

enum class task_transaction_kind : std::uint32_t {
    /**
     * @brief only out-of-transaction operations (including commit) are conducted by this task
     */
    none = 0,

    /**
     * @brief in-transaction operations are conducted by this task
     */
    in_transaction,

    /**
     * @brief task conducts in-transaction and other operations that require serialized execution
     */
    sticky,
};

/**
 * @brief returns string representation of the value.
 * @param value the target value
 * @return the corresponded string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(task_transaction_kind value) noexcept {
    using namespace std::string_view_literals;
    using kind = task_transaction_kind;
    switch (value) {
        case kind::none: return "none"sv;
        case kind::in_transaction: return "in_transaction"sv;
        case kind::sticky: return "sticky"sv;
    }
    std::abort();
}

/**
 * @brief appends string representation of the given value.
 * @param out the target output
 * @param value the target value
 * @return the output
 */
inline std::ostream& operator<<(std::ostream& out, task_transaction_kind value) {
    return out << to_string_view(value);
}

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
     * @brief accessor to transaction capability of the task
     * @return the flag indicates the transactional operations conducted by this task
     */
    [[nodiscard]] virtual task_transaction_kind transaction_capability() = 0;

protected:
    virtual std::ostream& write_to(std::ostream& out) const = 0;

public:
    friend std::ostream& operator<<(std::ostream& out, task const& value) {
        return value.write_to(out);
    }
};

}
