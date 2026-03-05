/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <ostream>
#include <string_view>

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>

#include "operator_kind.h"

namespace jogasaki::executor::process::impl {
class variable_table;
}

namespace jogasaki::executor::process::impl::ops {

enum class context_state {
    /**
     * @brief normal state; the operator body is executing without any pending yield.
     */
    running_operator_body,

    /**
     * @brief this operator directly yielded the worker thread.
     * @details on resume, the operator continues from its own yield point rather than
     *          restarting from the beginning of its processing logic.
     */
    yielding,

    /**
     * @brief this operator is waiting for a child (downstream) operator to complete.
     * @details set just before invoking process_record on a downstream operator.
     *          if the child yields, this state persists so that on the next invocation
     *          the upstream operator knows to skip re-fetching input data (e.g. iterator
     *          advance) and call the child again directly.
     *          reset to running_operator_body when the child returns normally.
     */
    calling_child,

    /**
     * @brief the operator with this context met error and is aborting/aborted
     */
    aborted,
};

/**
 * @brief returns string representation of the context_state.
 * @param state the target context_state
 * @return the corresponding string representation
 */
[[nodiscard]] constexpr inline std::string_view to_string_view(context_state state) noexcept {
    using namespace std::string_view_literals;
    switch (state) {
        case context_state::running_operator_body: return "running_operator_body"sv;
        case context_state::yielding: return "yielding"sv;
        case context_state::calling_child: return "calling_child"sv;
        case context_state::aborted: return "aborted"sv;
    }
    std::abort();
}

/**
 * @brief outputs context_state to stream.
 */
inline std::ostream& operator<<(std::ostream& out, context_state value) {
    return out << to_string_view(value);
}

/**
 * @brief relational operator base class
 */
class context_base {
public:
    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    context_base() = default;

    /**
     * @brief create new object
     * @param ctx the parent task context
     * @param input_variables the input variable table
     * @param output_variables the output variable table
     */
    context_base(
        class abstract::task_context* context,
        variable_table& input_variables,
        variable_table& output_variables,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    /**
     * @brief create new object
     * @param ctx the parent task context
     * @param variables the variable table used for both input/output
     */
    context_base(
        class abstract::task_context* context,
        variable_table& variables,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    context_base(context_base const& other) = default;
    context_base& operator=(context_base const& other) = default;
    context_base(context_base&& other) noexcept = default;
    context_base& operator=(context_base&& other) noexcept = default;

    /**
     * @brief destory the object
     */
    virtual ~context_base() = default;

    /**
     * @brief accessor for operator kind
     */
    [[nodiscard]] virtual operator_kind kind() const noexcept = 0;

    /**
     * @brief accessor to variables table for the scope where operator/context belongs
     */
    [[nodiscard]] variable_table& input_variables() const noexcept;

    /**
     * @brief setter of variables table
     * @param variables reference to the variables table
     */
    void input_variables(variable_table& variables) noexcept;

    /**
     * @brief accessor to variables table for the scope where operator/context belongs
     */
    [[nodiscard]] variable_table& output_variables() const noexcept;

    /**
     * @brief setter of variables table
     * @param variables reference to the variables table
     */
    void output_variables(variable_table& variables) noexcept;

    /**
     * @brief accessor to task context
     * @return the task context
     */
    [[nodiscard]] class abstract::task_context& task_context() noexcept;

    /**
     * @brief accessor to the memory resource used for context objects (e.g. stack of record objects)
     */
    [[nodiscard]] memory_resource* resource() const noexcept;

    /**
     * @brief accessor to the varlen memory resource referenced by context objects
     */
    [[nodiscard]] memory_resource* varlen_resource() const noexcept;

    /**
     * @brief subclass releases any resources acquired after context initialization
     */
    virtual void release() = 0;

    /**
     * @brief accessor for the context state
     */
    [[nodiscard]] context_state state() const noexcept;

    /**
     * @brief update the context state
     * @param state the new state of the context
     */
    void state(context_state state) noexcept;

    /**
     * @brief update the context state aborted
     */
    void abort() noexcept;

    /**
     * @brief returns whether the context state is aborted
     */
    [[nodiscard]] bool aborted() const noexcept {
        return state_ == context_state::aborted;
    }

    /**
     * @brief accessor to request context
     * @return the request context
     */
    [[nodiscard]] request_context* req_context() noexcept;

    /**
     * @brief Support for debugging, callable in GDB: cb->dump()
     */
    void dump() const noexcept;

private:
    class abstract::task_context* task_context_{};
    variable_table* input_variables_{};
    variable_table* output_variables_{};
    memory_resource* resource_{};
    memory_resource* varlen_resource_{};
    context_state state_{context_state::running_operator_body};
};

}  // namespace jogasaki::executor::process::impl::ops
