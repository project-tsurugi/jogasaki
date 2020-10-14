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

#include <jogasaki/executor/process/impl/task_context.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include "operator_kind.h"

namespace jogasaki::executor::process::impl {
class block_scope;
}
namespace jogasaki::executor::process::impl::ops {

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
    explicit context_base(
        class abstract::task_context* context,
        block_scope& variables
    ) :
        task_context_(context),
        variables_(std::addressof(variables))
    {}

    context_base(context_base const& other) = default;
    context_base& operator=(context_base const& other) = default;
    context_base(context_base&& other) noexcept = default;
    context_base& operator=(context_base&& other) noexcept = default;

    virtual ~context_base() = default;

    [[nodiscard]] virtual operator_kind kind() const noexcept = 0;

    [[nodiscard]] block_scope& variables() {
        return *variables_;
    }

    [[nodiscard]] class abstract::task_context& task_context() {
        return *task_context_;
    }

    void variables(block_scope& variables) {
        variables_ = std::addressof(variables);
    }

    [[nodiscard]] memory_resource* resource() {
        return resource_;
    }
    /**
     * @brief subclass releases any resources acquired after context initialization
     */
    virtual void release() = 0;
private:
    class abstract::task_context* task_context_{};
    block_scope* variables_{};
    memory_resource* resource_{};
};

}


