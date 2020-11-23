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

#include <takatori/util/downcast.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/object_creator.h>
#include <takatori/relation/step/take_group.h>
#include <takatori/descriptor/variable.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include "operator_base.h"
#include "take_group_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

namespace details {

struct cache_align take_group_field {
    meta::field_type type_{};
    std::size_t source_offset_{};
    std::size_t target_offset_{};
    std::size_t source_nullity_offset_{};
    std::size_t target_nullity_offset_{};
    bool nullable_{};
    bool is_key_{};
};

}

/**
 * @brief take_group operator
 */
class take_group : public record_operator {
public:
    using column = takatori::relation::step::take_group::column;

    friend class take_group_context;
    /**
     * @brief create empty object
     */
    take_group() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param order the exchange columns ordering information that assigns the field index of the input record. The index
     * can be used with record_meta to get field metadata.
     * @param reader_index the index that identifies the reader in the task context. This corresponds to the input port
     * number that the input exchange is connected.
     * @param downstream downstream operator that should be invoked with the output from this operation
     */
    take_group(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        meta::variable_order const& order,
        maybe_shared_ptr<meta::group_meta> meta,
        takatori::util::sequence_view<column const> columns,
        std::size_t reader_index,
        std::unique_ptr<operator_base> downstream = nullptr
    ) : record_operator(index, info, block_index),
        meta_(std::move(meta)),
        fields_(create_fields(meta_, order, columns)),
        reader_index_(reader_index),
        downstream_(std::move(downstream))
    {}

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<take_group_context>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<take_group_context>(
                index(),
                ctx.block_scope(block_index()),
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
    void operator()(take_group_context& ctx, abstract::task_context* context = nullptr) {
        auto target = ctx.variables().store().ref();
        if (! ctx.reader_) {
            auto r = ctx.task_context().reader(reader_index_);
            ctx.reader_ = r.reader<group_reader>();
        }
        auto resource = ctx.varlen_resource();
        while(ctx.reader_->next_group()) {
            utils::checkpoint_holder group_cp{resource};
            auto key = ctx.reader_->get_group();
            for(auto &f : fields_) {
                if (! f.is_key_) continue;
                utils::copy_field(f.type_, target, f.target_offset_, key, f.source_offset_, resource); // copy from outside process
            }
            bool first_record = true;
            while(ctx.reader_->next_member()) {
                utils::checkpoint_holder member_cp{resource};
                auto value = ctx.reader_->get_member();
                for(auto &f : fields_) {
                    if (f.is_key_) continue;
                    utils::copy_field(f.type_, target, f.target_offset_, value, f.source_offset_, resource); // copy from outside process
                }
                if (downstream_) {
                    unsafe_downcast<group_operator>(downstream_.get())->process_group(context, first_record);
                }
                first_record = false;
            }
        }
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::take_group;
    }

    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& meta() const noexcept {
        return meta_;
    }

private:
    maybe_shared_ptr<meta::group_meta> meta_{};
    std::vector<details::take_group_field> fields_{};
    std::size_t reader_index_{};
    std::unique_ptr<operator_base> downstream_{};

    [[nodiscard]] std::vector<details::take_group_field> create_fields(
        maybe_shared_ptr<meta::group_meta> const& meta,
        meta::variable_order const& order,
        takatori::util::sequence_view<column const> columns
    ) {
        std::vector<details::take_group_field> fields{};
        auto& key_meta = meta->key();
        auto& value_meta = meta->value();
        auto num_keys = key_meta.field_count();
        auto num_fields = num_keys+value_meta.field_count();
        BOOST_ASSERT(columns.size() <= num_fields);  //NOLINT // it's possible requested columns are only part of exchange fields
        fields.resize(columns.size());
        auto& vmap = block_info().value_map();
        for(auto&& c : columns) {
            auto [src_idx, is_key] = order.key_value_index(c.source());
            auto& target_info = vmap.at(c.destination());
            auto idx = src_idx + (is_key ? 0 : num_keys); // copy keys first, then values
            fields[idx]=details::take_group_field{
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

}


