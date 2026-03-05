/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "flatten.h"

#include <utility>
#include <boost/assert.hpp>

#include <takatori/util/downcast.h>
#include <takatori/util/infect_qualifier.h>

#include <jogasaki/executor/process/impl/ops/context_container.h>

#include "context_helper.h"
#include "flatten_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

flatten::flatten(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::unique_ptr<operator_base> downstream
) :
    group_operator(index, info, block_index),
    downstream_(std::move(downstream))
{}

operation_status flatten::process_group(abstract::task_context* context, bool last_member) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    (void)last_member;
    context_helper ctx{*context};
    auto* p = find_context<flatten_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<flatten_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status flatten::operator()(flatten_context& ctx, abstract::task_context* context) {
    if (ctx.aborted()) {
        return operation_status_kind::aborted;
    }

    // this op. simply transforms group into record, so actually there is nothing done here

    if (ctx.state() == context_state::calling_child) {
        VLOG_LP(log_trace) << "resuming flatten op. after downstream yield";
    }

    if (downstream_) {
        ctx.state(context_state::calling_child);
        auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
        if (st.kind() == operation_status_kind::yield) {
            return operation_status_kind::yield;
        }
        if (! st) {
            ctx.abort();
            return operation_status_kind::aborted;
        }
        ctx.state(context_state::running_operator_body);
    }
    return operation_status_kind::ok;
}

operator_kind flatten::kind() const noexcept {
    return operator_kind::flatten;
}

void flatten::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if (auto* p = find_context<flatten_context>(index(), ctx.contexts())) {
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

}


