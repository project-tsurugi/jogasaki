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
#include "filter.h"

#include <takatori/util/downcast.h>
#include <takatori/relation/filter.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include "operator_base.h"
#include "filter_context.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

filter::filter(
    operator_base::operator_index_type index,
    const processor_info& info,
    operator_base::block_index_type block_index,
    const takatori::scalar::expression& expression,
    std::unique_ptr<operator_base> downstream
) :
    record_operator(index, info, block_index),
    evaluator_(expression, info.compiled_info(), info.host_variables()),
    downstream_(std::move(downstream))
{}

operation_status filter::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<filter_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<filter_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status filter::operator()(filter_context& ctx, abstract::task_context* context) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    auto& vars = ctx.variables();
    auto resource = ctx.varlen_resource();
    bool res;
    {
        utils::checkpoint_holder cp{resource};
        res = evaluator_(vars, resource).to<bool>();
    }
    if (res) {
        if (downstream_) {
            if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
                ctx.abort();
                return {operation_status_kind::aborted};
            }
        }
    }
    return {};
}

operator_kind filter::kind() const noexcept {
    return operator_kind::filter;
}

void filter::finish(abstract::task_context* context) {
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

}


