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

#include <takatori/util/sequence_view.h>
#include <takatori/util/object_creator.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/project.h>
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
    ) :
        record_operator(index, info, block_index),
        downstream_(std::move(downstream))
    {
        for(auto&& c: columns) {
            evaluators_.emplace_back(c.value(), info.compiled_info());
            variables_.emplace_back(c.variable());
        }
    }

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<project_context>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<project_context>(
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
     * @details evaluate the column expression and populate the variables so that downstream can use them.
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     */
    void operator()(project_context& ctx, abstract::task_context* context = nullptr) {
        auto& scope = ctx.variables();
        // fill scope variables
        auto ref = scope.store().ref();
        auto& cinfo = compiled_info();
        for(std::size_t i=0, n = variables_.size(); i < n; ++i) {
            auto& v = variables_[i];
            auto info = scope.value_map().at(variables_[i]);
            auto& ev = evaluators_[i];
            auto result = ev(scope, ctx.varlen_resource()); // result resource will be deallocated at once by take/scan operator
            using t = takatori::type::type_kind;
            switch(cinfo.type_of(v).kind()) {
                case t::int4: copy_to<std::int32_t>(ref, info.value_offset(), result); break;
                case t::int8: copy_to<std::int64_t>(ref, info.value_offset(), result); break;
                case t::float4: copy_to<float>(ref, info.value_offset(), result); break;
                case t::float8: copy_to<double>(ref, info.value_offset(), result); break;
                case t::character: copy_to<accessor::text>(ref, info.value_offset(), result); break;
                default: fail();
            }
        }
        if (downstream_) {
            unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
        }
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::project;
    }

    void finish(abstract::task_context* context) override {
        if (downstream_) {
            unsafe_downcast<record_operator>(downstream_.get())->finish(context);
        }
    }
private:
    std::vector<expression::evaluator> evaluators_{};
    std::vector<takatori::descriptor::variable> variables_{};
    std::unique_ptr<operator_base> downstream_{};

    template <typename T>
    void copy_to(accessor::record_ref target_ref, std::size_t target_offset, expression::any src) {
        target_ref.set_value<T>(target_offset, src.to<T>());
    }
};

}


