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

    void process_record(operator_executor* parent) override {
        BOOST_ASSERT(parent != nullptr);  //NOLINT
        context_container& container = parent->contexts();
        auto* p = find_context<filter_context>(index(), container);
        if (! p) {
            p = parent->make_context<filter_context>(index(), parent->get_block_variables(block_index()), parent->resource());
        }
        (*this)(*p, parent);
    }
    /**
     * @brief conduct the filter operation
     * @details evaluate the filter condition and invoke downstream if the condition is met
     * @tparam Callback the callback object type responsible for dispatching the control to downstream
     * @param ctx context object for the execution
     * @param visitor the callback object to dispatch to downstream. Pass nullptr if no dispatch is needed (e.g. test.)
     */
    void operator()(filter_context& ctx, operator_executor* parent = nullptr) {
        auto& scope = ctx.variables();
        auto resource = ctx.resource();
        auto cp = resource->get_checkpoint();
        auto res = evaluator_(scope, resource).to<bool>();
        resource->deallocate_after(cp);
        if (res) {
            if (downstream_) {
                unsafe_downcast<record_operator>(downstream_.get())->process_record(parent);
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


