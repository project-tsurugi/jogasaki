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
#include "take_group.h"

#include <type_traits>
#include <utility>
#include <vector>
#include <boost/assert.hpp>

#include <takatori/util/downcast.h>
#include <takatori/util/infect_qualifier.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/take_group_context.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/lazy_checkpoint_holder.h>
#include <jogasaki/utils/validation.h>

#include "cancel_if_needed.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

take_group::take_group(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    meta::variable_order const& order,
    maybe_shared_ptr<meta::group_meta> meta,
    sequence_view<column const> columns,
    std::size_t reader_index,
    bool whole_group,
    std::unique_ptr<operator_base> downstream
) :
    record_operator(index, info, block_index),
    meta_(std::move(meta)),
    fields_(create_fields(meta_, order, columns)),
    reader_index_(reader_index),
    whole_group_(whole_group),
    downstream_(std::move(downstream))
{
    utils::assert_all_fields_nullable(meta_->key());
    utils::assert_all_fields_nullable(meta_->value());
}

operation_status take_group::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<take_group_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<take_group_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status take_group::process_empty_group(
    take_group_context& ctx,
    abstract::task_context* context
) {
    if (! downstream_) {
        return operation_status_kind::ok;
    }
    if (ctx.state() != context_state::calling_child) {
        auto cancel_enabled = utils::request_cancel_enabled(request_cancel_kind::take_group);
        if (cancel_enabled && cancel_if_needed(ctx)) {
            finish(context);
            return operation_status_kind::aborted;
        }
    }
    ctx.state(context_state::calling_child);
    auto st = unsafe_downcast<group_operator>(downstream_.get())->process_group(context, member_kind::empty);
    if (st.kind() == operation_status_kind::yield) {
        return operation_status_kind::yield;
    }
    if (st.kind() == operation_status_kind::aborted) {
        ctx.abort();
        finish(context);
        return operation_status_kind::aborted;
    }
    ctx.state(context_state::running_operator_body);
    return operation_status_kind::ok;
}

operation_status take_group::operator()(take_group_context& ctx, abstract::task_context* context) {  //NOLINT(readability-function-cognitive-complexity)
    assert_with_exception(ctx.state() != context_state::yielding, ctx.state());
    if (ctx.aborted()) {
        return operation_status_kind::aborted;
    }
    if (whole_group_ && context != nullptr && context->work_context() != nullptr) {
        // if whole_group is set and upstream shuffle has no record, send empty group to downstream
        context_helper helper{*context};
        if (helper.empty_input_from_shuffle()) {
            if(ctx.state() == context_state::calling_child) {
                // simply log we are resuming
                VLOG_LP(log_trace) << "resuming take_group op. (with empty input) after downstream yield";
            }
            auto st = process_empty_group(ctx, context);
            if (st.kind() != operation_status_kind::ok) {
                return st;
            }
            finish(context);
            return operation_status_kind::ok;
        }
    }
    auto target = ctx.output_variables().store().ref();
    auto cancel_enabled = utils::request_cancel_enabled(request_cancel_kind::take_group);
    auto resource = ctx.varlen_resource();
    if(ctx.state() == context_state::calling_child) {
        VLOG_LP(log_trace) << "resuming take_group op. after downstream yield";
        goto resume_calling_child;  //NOLINT
    }
    if (! ctx.reader_) {
        auto r = ctx.task_context().reader(reader_index_);
        ctx.reader_ = r.reader<io::group_reader>();
    }

    ctx.group_cp_.set_checkpoint();
    while(ctx.reader_->next_group()) {
        ctx.group_cp_.release();
        {
            auto key = ctx.reader_->get_group();
            for(auto &f : fields_) {
                if (! f.is_key_) continue;
                utils::copy_nullable_field(
                    f.type_,
                    target,
                    f.target_offset_,
                    f.target_nullity_offset_,
                    key,
                    f.source_offset_,
                    f.source_nullity_offset_,
                    resource
                );
            }
        }

        if(! ctx.reader_->next_member()) continue;
        ctx.has_next_ = true;
        while(ctx.has_next_) {
            if (cancel_enabled && cancel_if_needed(ctx)) {
                ctx.group_cp_.reset();
                finish(context);
                return operation_status_kind::aborted;
            }
            {
                auto value = ctx.reader_->get_member();
                for(auto &f : fields_) {
                    if (f.is_key_) continue;
                    utils::copy_nullable_field(
                        f.type_,
                        target,
                        f.target_offset_,
                        f.target_nullity_offset_,
                        value,
                        f.source_offset_,
                        f.source_nullity_offset_,
                        resource
                    );
                }
            }
            ctx.has_next_ = ctx.reader_->next_member();
resume_calling_child:
            if (downstream_) {
                ctx.state(context_state::calling_child);
                auto st = unsafe_downcast<group_operator>(
                    downstream_.get())->process_group(
                        context,
                        ctx.has_next_ ? member_kind::normal : member_kind::last_member);
                if (st.kind() == operation_status_kind::yield) {
                    // preserve ctx.group_cp_ and ctx.has_next_
                    return operation_status_kind::yield;
                }
                if (st.kind() == operation_status_kind::aborted) {
                    ctx.abort();
                    ctx.group_cp_.reset();
                    finish(context);
                    return operation_status_kind::aborted;
                }
                ctx.state(context_state::running_operator_body);
            }
        }
    }
    ctx.group_cp_.reset();
    finish(context);
    return operation_status_kind::ok;
}

operator_kind take_group::kind() const noexcept {
    return operator_kind::take_group;
}

const maybe_shared_ptr<meta::group_meta>& take_group::meta() const noexcept {
    return meta_;
}

void take_group::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper c{*context};
    if(auto* p = find_context<take_group_context>(index(), c.contexts())) {
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<group_operator>(downstream_.get())->finish(context);
    }
}

std::vector<details::take_group_field> take_group::create_fields(
    maybe_shared_ptr<meta::group_meta> const& meta,
    meta::variable_order const& order,
    sequence_view<const column> columns
) {
    std::vector<details::take_group_field> fields{};
    auto& key_meta = meta->key();
    auto& value_meta = meta->value();
    BOOST_ASSERT(order.size() == key_meta.field_count()+value_meta.field_count());  //NOLINT
    BOOST_ASSERT(order.key_count() == key_meta.field_count());  //NOLINT
    BOOST_ASSERT(columns.size() <= key_meta.field_count()+value_meta.field_count());  //NOLINT
                                                  // it's possible requested columns are only part of exchange fields
    fields.resize(columns.size());
    [[maybe_unused]] auto num_keys = 0;
    for(auto&& c : columns) {
        if(order.is_key(c.source())) {
            ++num_keys;
        }
    }
    for(std::size_t i=0, n=columns.size(); i < n; ++i) {
        auto& c = columns[i];
        auto [src_idx, is_key] = order.key_value_index(c.source());
        auto& target_info = block_info().at(c.destination());
        fields[i] = details::take_group_field{
            is_key ? key_meta.at(src_idx) : value_meta.at(src_idx),
            is_key ? key_meta.value_offset(src_idx) : value_meta.value_offset(src_idx),
            target_info.value_offset(),
            is_key ? key_meta.nullity_offset(src_idx) : value_meta.nullity_offset(src_idx),
            target_info.nullity_offset(),
            is_key ? key_meta.nullable(src_idx) : value_meta.nullable(src_idx),
            is_key
        };
    }
    return fields;
}

}  // namespace jogasaki::executor::process::impl::ops
