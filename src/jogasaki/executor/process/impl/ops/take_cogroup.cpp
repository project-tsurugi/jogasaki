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
#include "take_cogroup.h"

#include <memory>
#include <queue>
#include <type_traits>
#include <utility>
#include <boost/assert.hpp>

#include <takatori/util/downcast.h>
#include <takatori/util/infect_qualifier.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/validation.h>

#include "context_helper.h"
#include "take_cogroup_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

group_element::group_element(
    meta::variable_order const& order,
    maybe_shared_ptr<meta::group_meta> meta,
    sequence_view<column const> columns,
    std::size_t reader_index,
    variable_table_info const& block_info
) :
    order_(std::addressof(order)),
    meta_(std::move(meta)),
    reader_index_(reader_index),
    fields_(create_fields(meta_, *order_, columns, block_info)),
    key_meta_(std::addressof(meta_->key()))
{
    utils::assert_all_fields_nullable(meta_->key());
    utils::assert_all_fields_nullable(meta_->value());
}

// TODO consolidate with take_group::create_fields
std::vector<group_field> group_element::create_fields(
    maybe_shared_ptr<meta::group_meta> const& meta,
    meta::variable_order const& order,
    sequence_view<const column> columns,
    variable_table_info const& block_info
) {
    std::vector<group_field> fields{};
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
        auto& target_info = block_info.at(c.destination());
        fields[i] = group_field{
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

take_cogroup::take_cogroup(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::vector<group_element> groups,
    std::unique_ptr<operator_base> downstream
) : record_operator(index, info, block_index),
    groups_(std::move(groups)),
    downstream_(std::move(downstream))
{
    fields_.reserve(groups_.size());
    auto& key_meta = groups_[0].meta_->key();
    (void)key_meta;
    for(auto&& g : groups_) {
        // key meta are identical on all inputs (except value offset)
        BOOST_ASSERT(g.meta_->key() == key_meta);  //NOLINT
        fields_.emplace_back(g.fields_);
    }
}

operation_status take_cogroup::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<take_cogroup_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<take_cogroup_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status take_cogroup::operator()(take_cogroup_context& ctx, abstract::task_context* context) {  //NOLINT(readability-function-cognitive-complexity)
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    using iterator = data::iterable_record_store::iterator;
    if (ctx.readers_.empty()) {
        create_readers(ctx);
    }
    BOOST_ASSERT(ctx.readers_.size() == groups_.size());  //NOLINT

    enum class state {
        // @brief initial state
        init,

        // @brief all inputs keys are read and if reader is not on eof, queue entry is pushed with the key
        keys_filled,

        // @brief all inputs values are read and kept in the input stores
        values_filled,

        // @brief end of the state machine
        end,
    };

    state s{state::init};
    auto& inputs = ctx.inputs_;
    auto& queue = ctx.queue_;
    auto cancel_enabled = utils::request_cancel_enabled(request_cancel_kind::take_cogroup);
    while(s != state::end) {
        if(cancel_enabled && ctx.req_context()) {
            auto& res_src = ctx.req_context()->req_info().response_source();
            if(res_src && res_src->check_cancel()) {
                cancel_request(*ctx.req_context());
                ctx.abort();
                finish(context);
                return {operation_status_kind::aborted};
            }
        }
        switch(s) {
            case state::init:
                for(input_index idx = 0, n = inputs.size(); idx < n; ++idx) {
                    auto& in = inputs[idx];
                    if(in.read_next_key()) {
                        queue.emplace(idx);
                    } else {
                        BOOST_ASSERT(in.eof());  //NOLINT
                    }
                }
                s = state::keys_filled;
                break;
            case state::keys_filled: {
                if (queue.empty()) {
                    // all inputs are eof
                    s = state::end;
                    break;
                }
                auto idx = queue.top();
                queue.pop();
                inputs[idx].fill();
                if(inputs[idx].read_next_key()) {
                    queue.emplace(idx);
                }
                while(! queue.empty()) {
                    auto idx2 = queue.top();
                    if (idx2 == idx) break;
                    compare_info cinfo{
                        inputs[idx2].meta()->key(),
                        inputs[idx].meta()->key()
                    };
                    comparator key_comparator{
                        cinfo
                    };
                    if (key_comparator(inputs[idx2].next_key(), inputs[idx].current_key()) != 0) {
                        break;
                    }
                    queue.pop();
                    inputs[idx2].fill();
                    if(inputs[idx2].read_next_key()) {
                        queue.emplace(idx2);
                    }
                }
                s = state::values_filled;
                break;
            }
            case state::values_filled:
                if (downstream_) {
                    std::vector<group<iterator>> groups{};
                    groups.reserve(inputs.size());
                    for(std::size_t i = 0, n = inputs.size(); i < n; ++i) {
                        auto& in = inputs[i];
                        groups.emplace_back(
                            iterator_pair{in.begin(), in.end()},
                            groups_[i].fields_,
                            in.filled() ? in.current_key() : accessor::record_ref{},
                            in.meta()->value().record_size()
                        );
                    }
                    cogroup<iterator> cgrp{ groups };
                    if(auto st = unsafe_downcast<cogroup_operator<iterator>>(downstream_.get())->
                            process_cogroup(context, cgrp); !st) {
                        ctx.abort();
                        finish(context);
                        return {operation_status_kind::aborted};
                    }
                }
                for(auto&& in : inputs) {
                    in.reset_values();
                }
                s = state::keys_filled;
                break;
            case state::end:
                break;
        }
    }
    finish(context);
    return {};
}

operator_kind take_cogroup::kind() const noexcept {
    return operator_kind::take_cogroup;
}

void take_cogroup::finish(abstract::task_context* context) {
    if (! context) return;
    using iterator = data::iterable_record_store::iterator;
    context_helper c{*context};
    if (auto* p = find_context<take_cogroup_context>(index(), c.contexts())) {
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<cogroup_operator<iterator>>(downstream_.get())->finish(context);
    }
}

void take_cogroup::create_readers(take_cogroup_context& ctx) {
    for(auto&& g: groups_) {
        auto idx = g.reader_index_;
        auto* reader = ctx.task_context().reader(idx).reader<io::group_reader>();
        ctx.readers_.emplace_back(reader);
        ctx.queue_ = queue_type{
            details::group_input_comparator(&ctx.inputs_)
        };
        auto store = std::make_unique<data::iterable_record_store>(
            ctx.resource(),
            ctx.varlen_resource(),
            g.meta_->value_shared()
        );
        ctx.inputs_.emplace_back(
            *reader,
            std::move(store),
            ctx.resource(),
            ctx.varlen_resource(),
            g.meta_
        );
    }
}

}  // namespace jogasaki::executor::process::impl::ops
