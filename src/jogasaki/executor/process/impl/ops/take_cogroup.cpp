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
#include "take_cogroup.h"

#include <memory>
#include <queue>
#include <glog/logging.h>

#include <takatori/util/downcast.h>

#include <takatori/relation/step/take_cogroup.h>
#include <takatori/relation/step/take_group.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/utils/iterator_pair.h>
#include <jogasaki/utils/validation.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include "take_cogroup_context.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

group_element::group_element(const meta::variable_order& order, maybe_shared_ptr<meta::group_meta> meta,
    sequence_view<const column> columns, std::size_t reader_index, const block_scope_info& block_info) :
    order_(std::addressof(order)),
    meta_(std::move(meta)),
    reader_index_(reader_index),
    fields_(create_fields(meta_, *order_, columns, block_info))
{
    utils::assert_all_fields_nullable(meta_->key());
    utils::assert_all_fields_nullable(meta_->value());
}

std::vector<group_field>
group_element::create_fields(const maybe_shared_ptr<meta::group_meta>& meta, const meta::variable_order& order,
    sequence_view<const column> columns, const block_scope_info& block_info) {
    std::vector<group_field> fields{};
    auto& key_meta = meta->key();
    auto& value_meta = meta->value();
    auto num_keys = key_meta.field_count();
    BOOST_ASSERT(columns.size() <= num_keys+value_meta.field_count());  //NOLINT // it's possible requested columns are only part of exchange fields
    fields.resize(columns.size());
    auto& vmap = block_info.value_map();
    for(auto&& c : columns) {
        auto [src_idx, is_key] = order.key_value_index(c.source());
        auto& target_info = vmap.at(c.destination());
        auto idx = src_idx + (is_key ? 0 : num_keys); // copy keys first, then values
        fields[idx] = group_field{
            is_key ? key_meta.at(src_idx) : value_meta.at(src_idx),
            is_key ? key_meta.value_offset(src_idx) : value_meta.value_offset(src_idx),
            target_info.value_offset(),
            is_key ? key_meta.nullity_offset(src_idx) : value_meta.nullity_offset(src_idx),
            target_info.nullity_offset(),
            //TODO nullity
            false, // nullable
            is_key
        };
    }
    return fields;
}

take_cogroup::take_cogroup(operator_base::operator_index_type index, const processor_info& info,
    operator_base::block_index_type block_index, std::vector<group_element> groups,
    std::unique_ptr<operator_base> downstream) : record_operator(index, info, block_index),
    groups_(std::move(groups)),
    key_meta_(groups_[0].meta_->key_shared()),
    key_size_(key_meta_->record_size()),
    compare_info_(*key_meta_),
    downstream_(std::move(downstream))
{
    fields_.reserve(groups_.size());
    for(auto&& g : groups_) {
        // key meta are identical on all inputs
        BOOST_ASSERT(g.meta_->key() == *key_meta_);  //NOLINT
        fields_.emplace_back(g.fields_);
    }
}

void take_cogroup::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<take_cogroup_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<take_cogroup_context>(
            index(),
            ctx.block_scope(block_index()),
            key_meta_,
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    (*this)(*p, context);
}

void take_cogroup::operator()(take_cogroup_context& ctx, abstract::task_context* context) {
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
    while(s != state::end) {
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
                ctx.key_buf_.set(inputs[idx].current_key());
                auto key = ctx.key_buf_.ref();
                if(inputs[idx].read_next_key()) {
                    queue.emplace(idx);
                }
                comparator key_comparator{compare_info_};
                while(! queue.empty()) {
                    auto idx2 = queue.top();
                    if (key_comparator(inputs[idx2].next_key(), key) != 0) {
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
                    unsafe_downcast<cogroup_operator<iterator>>(downstream_.get())->process_cogroup(context, cgrp);
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
    for(auto* r : ctx.readers_) {
        r->release();
    }
    if (downstream_) {
        unsafe_downcast<cogroup_operator<iterator>>(downstream_.get())->finish(context);
    }
}

operator_kind take_cogroup::kind() const noexcept {
    return operator_kind::take_cogroup;
}

void take_cogroup::finish(abstract::task_context*) {
    fail();
}

void take_cogroup::create_readers(take_cogroup_context& ctx) {
    for(auto&& g: groups_) {
        auto idx = g.reader_index_;
        auto* reader = ctx.task_context().reader(idx).reader<group_reader>();
        ctx.readers_.emplace_back(reader);
        ctx.queue_ = queue_type{
            details::group_input_comparator(&ctx.inputs_, compare_info_)
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
}
