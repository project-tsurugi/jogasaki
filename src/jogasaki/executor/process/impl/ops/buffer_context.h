/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variables_view.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief context for the buffer operator
 * @details holds the resume state needed to continue from the correct downstream after a yield.
 */
class buffer_context : public context_base {
public:

    /**
     * @brief create empty object
     */
    buffer_context() = default;

    /**
     * @brief create new object
     * @param ctx the parent task context
     * @param variables view of the variable tables for this context's block
     * @param resource memory resource for context objects
     * @param varlen_resource varlen memory resource
     */
    buffer_context(
        class abstract::task_context* ctx,
        variables_view variables,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    /**
     * @see context_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see context_base::release()
     */
    void release() override;

    /**
     * @brief index of the downstream currently being called, or std::nullopt when not in calling_child state.
     */
    std::optional<std::size_t> current_child_{};  //NOLINT
};

}  // namespace jogasaki::executor::process::impl::ops
