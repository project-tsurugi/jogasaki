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

#include <jogasaki/request_context.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include "operator_kind.h"

namespace jogasaki::executor::process::impl {
class variable_table;
}

namespace jogasaki::executor::process::impl::ops {

enum class context_state {
    /**
     * @brief the operator with this context is running normally
     */
    active,

    /**
     * @brief the operator with this context met error and is aborting/aborted
     */
    abort,
};

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
    [[nodiscard]] variable_table& variables() const noexcept;

    /**
     * @brief setter of variables table
     * @param variables reference to the variables table
     */
    void variables(variable_table& variables) noexcept;

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
     * @brief update the context state aborted
     */
    [[nodiscard]] bool inactive() const noexcept {
        return state_ != context_state::active;
    }

    /**
     * @brief accessor to request context
     * @return the request context
     */
    [[nodiscard]] request_context* req_context() noexcept;

private:
    class abstract::task_context* task_context_{};
    variable_table* variables_{};
    memory_resource* resource_{};
    memory_resource* varlen_resource_{};
    context_state state_{context_state::active};
};

}


