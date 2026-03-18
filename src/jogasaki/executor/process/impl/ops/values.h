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

#include <cstddef>
#include <memory>
#include <vector>

#include <takatori/descriptor/variable.h>
#include <takatori/relation/values.h>
#include <takatori/type/data.h>
#include <takatori/util/downcast.h>

#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/processor_info.h>

#include "operator_base.h"
#include "values_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief field for the values operator
 */
struct values_field {
    meta::field_type type_{};                       //NOLINT
    bool nullable_{true};                           //NOLINT
    std::size_t offset_{};                          //NOLINT
    std::size_t nullity_offset_{};                  //NOLINT
    takatori::type::data const* target_type_{};     //NOLINT
};

} // namespace details

/**
 * @brief values operator - produces tuples from scalar expressions without any upstream scan.
 *
 * This operator corresponds to the SQL VALUES clause and literal SELECT (e.g. SELECT 1).
 * It iterates through its list of rows, evaluates the scalar expressions for each row,
 * populates the variable table, and dispatches to the downstream operator.
 */
class values : public record_operator {
public:
    friend class values_context;
    using memory_resource = context_base::memory_resource;

    /**
     * @brief create empty object
     */
    values() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param node the takatori values relation node
     * @param downstream downstream operator invoked after each row. Pass nullptr if not needed.
     */
    values(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        takatori::relation::values const& node,
        std::unique_ptr<operator_base> downstream = nullptr
    );

    /**
     * @brief create context (if needed) and iterate through all rows
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief process with context object
     * @details Iterates through all rows in the values node. For each row, evaluates
     *          the scalar expressions and stores the results into the variable table,
     *          then dispatches to the downstream operator.
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(values_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

private:
    std::vector<std::vector<expr::evaluator>> row_evaluators_{};
    std::vector<std::vector<takatori::type::data const*>> row_source_types_{};
    std::vector<details::values_field> fields_{};
    std::unique_ptr<operator_base> downstream_{};

    [[nodiscard]] std::vector<details::values_field> create_fields(
        std::vector<takatori::descriptor::variable> const& columns,
        processor_info const& pinfo
    );
};

}  // namespace jogasaki::executor::process::impl::ops
