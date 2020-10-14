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
#include <jogasaki/executor/process/impl/expression_evaluator.h>
#include "operator_base.h"
#include "project_context.h"

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief filter operator
 */
class project : public operator_base {
public:
    friend class project_context;

    /**
     * @brief create empty object
     */
    project() = default;

    /**
     * @brief create new object
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     */
    project(
        processor_info const& info,
        block_index_type block_index,
        takatori::tree::tree_fragment_vector<takatori::relation::project::column> const& columns,
        relation::expression const* downstream = nullptr
    ) :
        operator_base(info, block_index),
        downstream_(downstream)
    {
        for(auto&& c: columns) {
            evaluators_.emplace_back(c.value(), info.compiled_info());
            variables_.emplace_back(c.variable());
        }
    }

    /**
     * @brief conduct the operation
     * @param ctx context object for the execution
     */
    template <typename Callback = void>
    void operator()(project_context& ctx, Callback* visitor = nullptr) {
        auto& scope = ctx.variables();
        // fill scope variables
        auto ref = scope.store().ref();
        auto& cinfo = compiled_info();
        for(std::size_t i=0, n = variables_.size(); i < n; ++i) {
            auto& v = variables_[i];
            auto info = scope.value_map().at(variables_[i]);
            auto& ev = evaluators_[i];
            auto res = ev(scope, ctx.resource());
            using t = takatori::type::type_kind;
            switch(cinfo.type_of(v).kind()) {
                case t::int4: copy_to<std::int32_t>(ref, info.value_offset(), res); break;
                case t::int8: copy_to<std::int64_t>(ref, info.value_offset(), res); break;
                case t::float4: copy_to<float>(ref, info.value_offset(), res); break;
                case t::float8: copy_to<double>(ref, info.value_offset(), res); break;
                default: fail();
            }
        }
        if constexpr (!std::is_same_v<Callback, void>) {
            if (visitor && downstream_) {
                dispatch(*visitor, *downstream_);
            }
        }
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::project;
    }

private:
    std::vector<expression_evaluator> evaluators_{};
    std::vector<takatori::descriptor::variable> variables_{};
    relation::expression const* downstream_{};

    template <typename T>
    void copy_to(accessor::record_ref target_ref, std::size_t target_offset, any src) {
        target_ref.set_value<T>(target_offset, src.to<T>());
    }
};

}


