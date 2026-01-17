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
#include <memory>
#include <vector>

#include <takatori/relation/apply.h>
#include <takatori/util/downcast.h>

#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/meta/field_type.h>

#include "apply_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief field information for apply operator.
 * @details contains pre-computed field metadata to avoid repeated lookups.
 */
struct apply_field {
    meta::field_type type_{};
    std::size_t value_offset_{};
    std::size_t nullity_offset_{};
    std::size_t pos_{};
};

}  // namespace details

/**
 * @brief apply operator for table-valued function application.
 * @details this operator implements the APPLY (CROSS/OUTER) operation,
 *          which calls a table-valued function for each input row and
 *          joins the result with the input.
 */
class apply : public record_operator {
public:
    friend class apply_context;
    using memory_resource = context_base::memory_resource;

    /**
     * @brief constructs an empty operator.
     */
    apply() = default;

    /**
     * @brief constructs a new operator.
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param operator_kind the kind of apply operation (cross or outer)
     * @param function_info the table-valued function to call
     * @param columns the output column definitions from the apply node
     * @param arguments the argument expressions for the function
     * @param downstream the downstream operator invoked after this operation
     */
    apply(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        takatori::relation::apply_kind operator_kind,
        function::table_valued_function_info const* function_info,
        std::vector<takatori::relation::details::apply_column> const& columns,
        std::vector<expr::evaluator> arguments,
        std::unique_ptr<operator_base> downstream = nullptr
    );

    /**
     * @brief creates context (if needed) and processes record.
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief processes record with context object.
     * @details calls the table-valued function and joins results with input row.
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require
     * @return status of the operation
     */
    operation_status operator()(apply_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

private:
    takatori::relation::apply_kind operator_kind_{};
    function::table_valued_function_info const* function_info_{};
    std::vector<details::apply_field> fields_{};
    std::vector<expr::evaluator> argument_evaluators_{};
    std::unique_ptr<operator_base> downstream_{};

    /**
     * @brief creates the field information for output columns.
     * @param columns the output column definitions from the apply node
     * @return vector of field information
     */
    [[nodiscard]] std::vector<details::apply_field> create_fields(
        std::vector<takatori::relation::details::apply_column> const& columns
    );

    /**
     * @brief evaluates the function arguments.
     * @param ctx the apply context
     * @param args the output vector to store evaluated arguments
     * @return true if evaluation succeeded, false otherwise
     */
    bool evaluate_arguments(apply_context& ctx, std::vector<data::any>& args);

    /**
     * @brief assigns the sequence values to output variables.
     * @param ctx the apply context
     * @param sequence the sequence of values to assign
     * @return true if assignment succeeded, false otherwise
     */
    bool assign_sequence_to_variables(apply_context& ctx, data::any_sequence const& sequence);

    /**
     * @brief assigns NULL values to all output variables (for OUTER APPLY).
     * @param ctx the apply context
     */
    void assign_null_to_variables(apply_context& ctx);
};

}  // namespace jogasaki::executor::process::impl::ops
