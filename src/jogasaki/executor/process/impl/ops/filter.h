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

#include <memory>
#include <vector>

#include <takatori/relation/filter.h>
#include <takatori/scalar/expression.h>

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>

#include "filter_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief filter operator
 */
class filter : public record_operator {
public:
    friend class filter_context;

    /**
     * @brief create empty object
     */
    filter() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param expression expression used as filter condition
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    filter(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        takatori::scalar::expression const& expression,
        std::unique_ptr<operator_base> downstream = nullptr
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details evaluate the filter condition and invoke downstream if the condition is met
     * @param ctx context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(filter_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

private:
    expression::evaluator evaluator_{};
    std::unique_ptr<operator_base> downstream_{};
};


}
