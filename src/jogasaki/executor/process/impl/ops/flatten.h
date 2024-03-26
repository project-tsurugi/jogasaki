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

#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/processor_info.h>

#include "flatten_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

/**
 * @brief flatten operator
 */
class flatten : public group_operator {
public:
    friend class flatten_context;

    /**
     * @brief create empty object
     */
    flatten() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    flatten(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::unique_ptr<operator_base> downstream = nullptr
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @param last_member specify whether the current member is the last within the group
     * @return status of the operation
     */
    operation_status process_group(abstract::task_context* context, bool last_member) override;

    /**
     * @brief process record with context object
     * @details this operation is almost no-op because take_group already took records and assigned variables
     * @param ctx context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(flatten_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

private:
    std::unique_ptr<operator_base> downstream_{};
};

}


