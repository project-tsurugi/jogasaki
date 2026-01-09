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

#include <memory>
#include <vector>

#include <takatori/relation/apply.h>

#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>

#include "context_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief context for the apply operator.
 * @details this context holds the state for apply operator execution,
 *          including the current sequence stream and position.
 */
class apply_context : public context_base {
public:
    friend class apply;

    /**
     * @brief default constructor is deleted to own evaluator context.
     */
    apply_context() = delete;

    /**
     * @brief constructs a new context.
     * @param ctx the parent task context
     * @param variables the variable table for input/output
     * @param resource the memory resource for context objects
     * @param varlen_resource the memory resource for variable-length data
     */
    apply_context(
        class abstract::task_context* ctx,
        variable_table& variables,
        memory_resource* resource,
        memory_resource* varlen_resource
    );

    /**
     * @brief returns the kind of this context.
     * @return operator_kind::apply
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @brief releases any resources acquired after context initialization.
     */
    void release() override;

private:
    std::unique_ptr<data::any_sequence_stream> stream_{};
    bool has_output_{false};
    std::vector<data::any> args_{};
    expr::evaluator_context evaluator_context_;
};

}  // namespace jogasaki::executor::process::impl::ops
