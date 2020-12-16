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

#include <vector>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/process/impl/block_scope.h>
#include "operator_base.h"
#include "flatten_context.h"

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
    ) :
        group_operator(index, info, block_index),
        downstream_(std::move(downstream))
    {}

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @param last_member specify whether the current member is the last within the group
     */
    void process_group(abstract::task_context* context, bool last_member) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        (void)last_member;
        context_helper ctx{*context};
        auto* p = find_context<flatten_context>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<flatten_context>(
                index(),
                ctx.block_scope(block_index()),
                ctx.resource(),
                ctx.varlen_resource()
            );
        }
        (*this)(*p, context);
    }

    /**
     * @brief process record with context object
     * @details this operation is almost no-op because take_group already took records and assigned variables
     * @param ctx context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     */
    void operator()(flatten_context& ctx, abstract::task_context* context = nullptr) {
        (void)ctx;
        if (downstream_) {
            unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
        }
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::flatten;
    }

    void finish(abstract::task_context* context) override {
        if (downstream_) {
            unsafe_downcast<record_operator>(downstream_.get())->finish(context);
        }
    }
private:
    std::unique_ptr<operator_base> downstream_{};
};

}


