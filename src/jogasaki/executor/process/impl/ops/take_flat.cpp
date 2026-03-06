/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "take_flat.h"

#include <type_traits>
#include <utility>
#include <vector>
#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/util/downcast.h>
#include <takatori/util/infect_qualifier.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/exchange/forward/reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/io/record_reader.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/take_flat_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/validation.h>

#include "cancel_if_needed.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

take_flat::take_flat(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    meta::variable_order const& order,
    maybe_shared_ptr<meta::record_meta> meta,
    sequence_view<column const> columns,
    std::size_t reader_index,
    std::unique_ptr<operator_base> downstream
) : record_operator(index, info, block_index),
    meta_(std::move(meta)),
    fields_(create_fields(meta_, order, columns)),
    reader_index_(reader_index),
    downstream_(std::move(downstream))
{
    utils::assert_all_fields_nullable(*meta_);
}

operation_status take_flat::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<take_flat_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<take_flat_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status take_flat::operator()(take_flat_context& ctx, abstract::task_context* context) {  //NOLINT(readability-function-cognitive-complexity)
    assert_with_exception(ctx.state() != context_state::yielding, ctx.state());
    if (ctx.aborted()) {
        return operation_status_kind::aborted;
    }
    auto target = ctx.output_variables().store().ref();
    auto resource = ctx.varlen_resource();
    auto cancel_enabled = utils::request_cancel_enabled(request_cancel_kind::take_flat);
    if (ctx.state() == context_state::calling_child) {
        VLOG_LP(log_trace) << "resuming take_flat op. after downstream yield";
        goto resume_calling_child;  //NOLINT
    }
    if (! ctx.reader_) {
        auto r = ctx.task_context().reader(reader_index_);
        ctx.reader_ = r.reader<io::record_reader>();
    }
    ctx.cp_.set_checkpoint();
    while(true) {
        // even if reader is not active loop next_record through all records and check is_active later
        ctx.is_active_ = ctx.reader_->source_active();
        while(ctx.reader_->next_record()) {
            ctx.cp_.reset();
            if (cancel_enabled && cancel_if_needed(ctx)) {
                finish(context);
                return operation_status_kind::aborted;
            }
            {
                auto source = ctx.reader_->get_record();
                for(auto &f : fields_) {
                    utils::copy_nullable_field(
                        f.type_,
                        target,
                        f.target_offset_,
                        f.target_nullity_offset_,
                        source,
                        f.source_offset_,
                        f.source_nullity_offset_,
                        resource
                    );
                }
            }

resume_calling_child:

            if (downstream_) {
                ctx.state(context_state::calling_child);
                auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
                if (st.kind() == operation_status_kind::yield) {
                    return operation_status_kind::yield;
                }
                if (st.kind() == operation_status_kind::aborted) {
                    ctx.cp_.reset();
                    ctx.abort();
                    finish(context);
                    return operation_status_kind::aborted;
                }
                ctx.state(context_state::running_operator_body);
            }
        }
        if(! ctx.is_active_) {
            break;
        }
        // keep running while reader is active even if the record is not available right now,
        // expecting upstream writes new one soon
    }
    ctx.cp_.reset();
    finish(context);
    return operation_status_kind::ok;
}

operator_kind take_flat::kind() const noexcept {
    return operator_kind::take_flat;
}

const maybe_shared_ptr<meta::record_meta>& take_flat::meta() const noexcept {
    return meta_;
}

void take_flat::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper c{*context};
    if(auto* p = find_context<take_flat_context>(index(), c.contexts())) {
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

std::vector<details::take_flat_field> take_flat::create_fields(
    maybe_shared_ptr<meta::record_meta> const& meta,
    meta::variable_order const& order,
    sequence_view<column const> columns
) {
    std::vector<details::take_flat_field> fields{};
    fields.resize(meta->field_count());
    for(auto&& c : columns) {
        auto ind = order.index(c.source());
        auto& info = block_info().at(c.destination());
        fields[ind] = details::take_flat_field{
            meta->at(ind),
            meta->value_offset(ind),
            info.value_offset(),
            meta->nullity_offset(ind),
            info.nullity_offset(),
            meta->nullable(ind)
        };
    }
    return fields;
}

}  // namespace jogasaki::executor::process::impl::ops
