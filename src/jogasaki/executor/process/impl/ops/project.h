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
#include <memory>
#include <vector>

#include <takatori/descriptor/variable.h>
#include <takatori/relation/project.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/any.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/ops/context_base.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/processor_info.h>

#include "operator_base.h"
#include "project_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;
/**
 * @brief project operator
 */
class project : public record_operator {
public:
    friend class project_context;
    using memory_resource = context_base::memory_resource;

    /**
     * @brief create empty object
     */
    project() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param columns list of columns newly added by this project operation
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    project(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        takatori::tree::tree_fragment_vector<takatori::relation::project::column> const& columns,
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
     * @details evaluate the column expression and populate the variables so that downstream can use them.
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(project_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

private:
    std::vector<expression::evaluator> evaluators_{};
    std::vector<takatori::descriptor::variable> variables_{};
    std::unique_ptr<operator_base> downstream_{};

    template <typename T>
    void copy_to(accessor::record_ref target_ref, std::size_t target_offset, data::any src) {
        target_ref.set_value<T>(target_offset, src.to<T>());
    }
};

}


