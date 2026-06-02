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

#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/processor_info.h>

#include "buffer_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

/**
 * @brief buffer operator
 * @details Fans out a single input record to N downstream operator chains in order.
 *     When a downstream yields the worker thread, the buffer remembers which downstream
 *     was active so it can resume from the same downstream on re-entry.
 */
class buffer : public record_operator {
public:
    /**
     * @brief create empty object
     */
    buffer() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param downstreams the downstream operator chains invoked for each incoming record
     */
    buffer(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::vector<std::unique_ptr<operator_base>> downstreams
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @param ctx operator context object for the execution
     * @param context task context for the downstreams, can be nullptr if none require it
     * @return status of the operation
     */
    operation_status operator()(buffer_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

private:
    std::vector<std::unique_ptr<operator_base>> downstreams_{};
};

}  // namespace jogasaki::executor::process::impl::ops
