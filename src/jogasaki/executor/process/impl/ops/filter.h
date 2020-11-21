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
#include <takatori/util/sequence_view.h>
#include <takatori/util/object_creator.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/relation/filter.h>
#include <takatori/descriptor/variable.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/impl/block_scope.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include "operator_base.h"
#include "filter_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

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
    ) : record_operator(index, info, block_index),
        evaluator_(expression, info.compiled_info()),
        downstream_(std::move(downstream))
    {}

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<filter_context>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<filter_context>(
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
     * @details evaluate the filter condition and invoke downstream if the condition is met
     * @param ctx context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     */
    void operator()(filter_context& ctx, abstract::task_context* context = nullptr) {
        auto& scope = ctx.variables();
        auto resource = ctx.varlen_resource();
        bool res;
        {
            utils::checkpoint_holder cp{resource};
            res = evaluator_(scope, resource).to<bool>();
        }
        if (res) {
            if (downstream_) {
                unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
            }
        }
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::filter;
    }

private:
    expression::evaluator evaluator_{};
    std::unique_ptr<operator_base> downstream_{};
};

}


