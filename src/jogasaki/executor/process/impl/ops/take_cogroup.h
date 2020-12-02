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

#include <memory>
#include <queue>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/downcast.h>

#include <takatori/relation/step/take_cogroup.h>
#include <takatori/relation/step/take_group.h>

#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/executor/common/task.h>
#include <jogasaki/executor/group_reader.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/utils/iterator_pair.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/validation.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include "take_cogroup_context.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;
using takatori::util::sequence_view;

class group_element {
public:
    using column = takatori::relation::step::take_group::column;

    group_element(
        meta::variable_order const& order,
        maybe_shared_ptr<meta::group_meta> meta,
        sequence_view<column const> columns,
        std::size_t reader_index,
        block_scope_info const& block_info
    ) :
        order_(std::addressof(order)),
        meta_(std::move(meta)),
        reader_index_(reader_index),
        fields_(create_fields(meta_, *order_, columns, block_info))
    {
        utils::assert_all_fields_nullable(meta_->key());
        utils::assert_all_fields_nullable(meta_->value());
    }

    meta::variable_order const* order_{}; //NOLINT
    maybe_shared_ptr<meta::group_meta> meta_{}; //NOLINT
    std::size_t reader_index_{}; //NOLINT
    std::vector<group_field> fields_{}; //NOLINT

    [[nodiscard]] std::vector<group_field> create_fields(
        maybe_shared_ptr<meta::group_meta> const& meta,
        meta::variable_order const& order,
        takatori::util::sequence_view<column const> columns,
        block_scope_info const& block_info
    ) {
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
};

class take_cogroup : public record_operator {
public:
    using iterator_pair = utils::iterator_pair<details::group_input::iterator>;
    using queue_type = take_cogroup_context::queue_type;
    using input_index = take_cogroup_context::input_index;

    take_cogroup(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::vector<group_element> groups,
        std::unique_ptr<operator_base> downstream = nullptr
    ) : record_operator(index, info, block_index),
        groups_(std::move(groups)),
        key_meta_(groups_[0].meta_->key_shared()),
        key_size_(key_meta_->record_size()),
        key_comparator_(key_meta_.get()),
        downstream_(std::move(downstream))
    {
        fields_.reserve(groups_.size());
        for(auto&& g : groups_) {
            // key meta are identical on all inputs
            BOOST_ASSERT(g.meta_->key() == *key_meta_);  //NOLINT
            fields_.emplace_back(g.fields_);
        }
    }

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override {
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

    /**
     * @brief process record with context object
     * @details process record, fill variables, and invoke downstream
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     */
    void operator()(take_cogroup_context& ctx, abstract::task_context* context = nullptr) {
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
                    while(! queue.empty()) {
                        auto idx2 = queue.top();
                        if (key_comparator_(inputs[idx2].next_key(), key) != 0) {
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
                        using iterator = data::iterable_record_store::iterator;
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
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::take_cogroup;
    }

private:
    std::vector<group_element> groups_{};
    maybe_shared_ptr<meta::record_meta> key_meta_{};
    std::size_t key_size_{};
    comparator key_comparator_{};
    std::unique_ptr<operator_base> downstream_{};
    std::vector<sequence_view<group_field>> fields_{};

    void create_readers(take_cogroup_context& ctx) {
        for(auto&& g: groups_) {
            auto idx = g.reader_index_;
            auto* reader = ctx.task_context().reader(idx).reader<group_reader>();
            ctx.readers_.emplace_back(reader);
            ctx.queue_ = queue_type{
                details::group_input_comparator(&ctx.inputs_, key_meta_.get())
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
};

}
