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
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/relation/step/offer.h>
#include <takatori/descriptor/variable.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/impl/block_scope.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/validation.h>
#include "operator_base.h"
#include "offer_context.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

struct cache_align offer_field {
    meta::field_type type_{};
    std::size_t source_offset_{};
    std::size_t target_offset_{};
    std::size_t source_nullity_offset_{};
    std::size_t target_nullity_offset_{};
    bool nullable_{};
};

}

/**
 * @brief offer operator
 */
class offer : public record_operator {
public:
    friend class offer_context;

    using column = takatori::relation::step::offer::column;

    /**
     * @brief create empty object
     */
    offer() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param order the exchange columns ordering information that assigns the field index of the output record. The index
     * can be used with record_meta to get field metadata.
     * @param meta the record metadata of the output record. This information is typically provided by the downstream exchange.
     * @param columns mapping information between variables and exchange columns
     * @param writer_index the index that identifies the writer in the task context. This corresponds to the output port
     * number that the output exchange is connected.
     */
    offer(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        meta::variable_order const& order,
        maybe_shared_ptr<meta::record_meta> meta,
        takatori::util::sequence_view<column const> columns,
        std::size_t writer_index
    ) : record_operator(index, info, block_index),
        meta_(std::move(meta)),
        fields_(create_fields(meta_, order, columns)),
        writer_index_(writer_index)
    {
        utils::assert_all_fields_nullable(*meta_);
    }

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<offer_context>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<offer_context>(
                index(),
                meta(),
                ctx.block_scope(block_index()),
                ctx.resource(),
                ctx.varlen_resource()
            );
        }
        (*this)(*p);
    }

    /**
     * @brief process record with context object
     * @param ctx operator context object for the execution
     */
    void operator()(offer_context& ctx) {
        auto target = ctx.store_.ref();
        auto source = ctx.variables().store().ref();
        for(auto &f : fields_) {
            utils::copy_nullable_field(f.type_, target, f.target_offset_, f.target_nullity_offset_, source, f.source_offset_, f.source_nullity_offset_);
        }

        if (!ctx.writer_) {
            ctx.writer_ = ctx.task_context().downstream_writer(writer_index_);
        }
        ctx.writer_->write(target);
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::offer;
    }

    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

    void finish(abstract::task_context* context) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<offer_context>(index(), ctx.contexts());
        if (p && p->writer_) {
            p->writer_->flush();
        }
    }
private:
    maybe_shared_ptr<meta::record_meta> meta_{};
    std::vector<details::offer_field> fields_{};
    std::size_t writer_index_{};

    [[nodiscard]] std::vector<details::offer_field> create_fields(
        maybe_shared_ptr<meta::record_meta> const& meta,
        meta::variable_order const& order,
        takatori::util::sequence_view<column const> columns
    ) {
        std::vector<details::offer_field> fields{};
        fields.resize(meta->field_count());
        auto& vmap = block_info().value_map();
        for(auto&& c : columns) {
            auto ind = order.index(c.destination());
            auto& info = vmap.at(c.source());
            fields[ind] = details::offer_field{
                meta->at(ind),
                info.value_offset(),
                meta->value_offset(ind),
                info.nullity_offset(),
                meta->nullity_offset(ind),
                meta->nullable(ind)
            };
        }
        return fields;
    }
};

}


