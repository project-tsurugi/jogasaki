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
#include "buffer.h"

#include <utility>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/utils/assert.h>

#include "buffer_context.h"
#include "context_helper.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

buffer::buffer(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::vector<std::unique_ptr<operator_base>> downstreams
) :
    record_operator(index, info, block_index),
    downstreams_(std::move(downstreams))
{}

operation_status buffer::process_record(abstract::task_context* context) {
    assert_with_exception(context != nullptr, context);
    context_helper ctx{*context};
    auto* p = find_context<buffer_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<buffer_context>(
            index(),
            block_index(),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status buffer::operator()(buffer_context& ctx, abstract::task_context* context) {
    if (ctx.aborted()) {
        return operation_status_kind::aborted;
    }
    std::size_t cur = 0;
    if (ctx.state() == context_state::calling_child) {
        assert_with_exception(ctx.current_child_.has_value());
        cur = ctx.current_child_.value();
        VLOG_LP(log_trace) << "resuming buffer op. after downstream yield";
    }

    ctx.state(context_state::calling_child);
    for (std::size_t i = cur; i < downstreams_.size(); ++i) {
        ctx.current_child_ = i;
        auto st = unsafe_downcast<record_operator>(downstreams_[i].get())->process_record(context);
        if (st.kind() == operation_status_kind::yield) {
            return operation_status_kind::yield;
        }
        if (st.kind() == operation_status_kind::aborted) {
            ctx.abort();
            return operation_status_kind::aborted;
        }
    }
    ctx.state(context_state::running_operator_body);
    ctx.current_child_ = std::nullopt;
    return operation_status_kind::ok;
}

operator_kind buffer::kind() const noexcept {
    return operator_kind::buffer;
}

void buffer::finish(abstract::task_context* context) {
    if (! context) {
        return;
    }
    context_helper ctx{*context};
    if (auto* p = find_context<buffer_context>(index(), ctx.contexts())) {
        p->release();
    }
    for (auto& downstream : downstreams_) {
        if (downstream) {
            unsafe_downcast<record_operator>(downstream.get())->finish(context);
        }
    }
}

}  // namespace jogasaki::executor::process::impl::ops
