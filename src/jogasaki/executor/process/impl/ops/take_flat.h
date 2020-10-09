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

#include <takatori/util/sequence_view.h>
#include <takatori/util/object_creator.h>
#include <takatori/relation/step/offer.h>
#include <takatori/relation/step/dispatch.h>
#include <takatori/descriptor/variable.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/utils/interference_size.h>
#include "operator_base.h"
#include "take_flat_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::relation::step::dispatch;
using takatori::util::maybe_shared_ptr;

namespace details {

struct cache_align take_flat_field {
    meta::field_type type_{};
    std::size_t source_offset_{};
    std::size_t target_offset_{};
    std::size_t source_nullity_offset_{};
    std::size_t target_nullity_offset_{};
    bool nullable_{};
};

}

/**
 * @brief take_flat operator
 */
class take_flat : public operator_base {
public:
    friend class take_flat_context;

    using column = takatori::relation::step::take_flat::column;

    /**
     * @brief create empty object
     */
    take_flat() = default;

    /**
     * @brief create new object
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param order the exchange columns ordering information that assigns the field index of the input record. The index
     * can be used with record_meta to get field metadata.
     * @param meta the record metadata of the record. This information is typically provided by the upstream exchange.
     * @param columns mapping information between exchange columns and variables
     * @param reader_index the index that identifies the reader in the task context. This corresponds to the input port
     * number that the input exchange is connected.
     * @param downstream downstream operator that should be invoked with the output from this operation
     */
    take_flat(
        processor_info const& info,
        block_index_type block_index,
        meta::variable_order const& order,
        maybe_shared_ptr<meta::record_meta> meta,
        takatori::util::sequence_view<column const> columns,
        std::size_t reader_index,
        relation::expression const* downstream = nullptr
    ) : operator_base(info, block_index),
        meta_(std::move(meta)),
        fields_(create_fields(meta_, order, columns)),
        reader_index_(reader_index),
        downstream_(downstream)
    {}

    /**
     * @brief conduct the operation
     * @tparam Callback type of parent operator executor, which can be called via takatori::relation::step::dispatch.
     * @param ctx context object for the execution
     * @param visitor the callback object that should be invoked to process output of this operation. Pass nullptr if
     * this operation is executed stand-alone and no subsequent processing is needed (e.g. in testcases).
     */
    template <class Callback = void>
    void operator()(take_flat_context& ctx, Callback* visitor = nullptr) {
        auto target = ctx.variables().store().ref();
        if (! ctx.reader_) {
            auto r = ctx.task_context().reader(reader_index_);
            ctx.reader_ = r.reader<record_reader>();
        }
        while(ctx.reader_->next_record()) {
            auto source = ctx.reader_->get_record();
            for(auto &f : fields_) {
                utils::copy_field(f.type_, target, f.target_offset_, source, f.source_offset_);
            }
            if constexpr (!std::is_same_v<Callback, void>) {
                if (downstream_ && visitor) {
                    dispatch(*visitor, *downstream_);

                }
            }
        }
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::take_flat;
    }

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::vector<details::take_flat_field> fields_{};
    std::size_t reader_index_{};
    relation::expression const* downstream_{};

    [[nodiscard]] std::vector<details::take_flat_field> create_fields(
        maybe_shared_ptr<meta::record_meta> const& meta,
        meta::variable_order const& order,
        takatori::util::sequence_view<column const> columns
    ) {
        std::vector<details::take_flat_field> fields{};
        fields.resize(meta->field_count());
        auto& vmap = block_info().value_map();
        for(auto&& c : columns) {
            auto ind = order.index(c.source());
            auto& info = vmap.at(c.destination());
            fields[ind] = details::take_flat_field{
                meta->at(ind),
                meta->value_offset(ind),
                info.value_offset(),
                meta->nullity_offset(ind),
                info.nullity_offset(),
                //TODO nullity
                false // nullable
            };
        }
        return fields;
    }
};

}


