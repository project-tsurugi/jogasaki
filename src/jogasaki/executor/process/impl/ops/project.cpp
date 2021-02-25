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
#include "project.h"


#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/downcast.h>
#include <takatori/relation/project.h>

#include "operator_base.h"
#include "project_context.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

ops::project::project(operator_base::operator_index_type index, const processor_info& info,
    operator_base::block_index_type block_index,
    const takatori::tree::tree_fragment_vector<takatori::relation::project::column>& columns,
    std::unique_ptr<operator_base> downstream) :
    record_operator(index, info, block_index),
    downstream_(std::move(downstream))
{
    for(auto&& c: columns) {
        evaluators_.emplace_back(c.value(), info.compiled_info());
        variables_.emplace_back(c.variable());
    }
}

operation_status project::process_record(abstract::task_context* context) {
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
    return (*this)(*p, context);
}

operation_status project::operator()(project_context& ctx, abstract::task_context* context) {
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
        ref.set_null(info.nullity_offset(), ! result.has_value());
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
    return {};
}

operator_kind project::kind() const noexcept {
    return operator_kind::project;
}

void project::finish(abstract::task_context* context) {
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

}


