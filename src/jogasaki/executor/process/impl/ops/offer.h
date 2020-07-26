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
#include <takatori/descriptor/variable.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/data/record_store.h>
#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/impl/block_scope.h>
#include <jogasaki/executor/process/impl/ops/process_io_map.h>
#include <jogasaki/utils/copy_field_data.h>
#include "operator_base.h"
#include "offer_context.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

struct offer_field {
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
class offer : public operator_base {
public:
    friend class offer_context;

    using column = takatori::relation::step::offer::column;
    /**
     * @brief create empty object
     */
    offer() = default;

    /**
     * @brief create new object
     */
    offer(
        processor_info const& info,
        block_index_type block_index,
        meta::variable_order const& order,
        std::vector<column, takatori::util::object_allocator<column>> const& columns,
        std::size_t writer_index
    ) : operator_base(info, block_index),
        meta_(create_meta(info, order, columns)),
        fields_(create_fields(meta_, order, columns)),
        writer_index_(writer_index)
    {}

    /**
     * @brief create new object
     */
    offer(
        processor_info const& info,
        block_index_type block_index,
        std::shared_ptr<meta::record_meta> meta,
        std::vector<details::offer_field> fields,
        std::size_t writer_index
    ) : operator_base(info, block_index),
        meta_(std::move(meta)),
        fields_(std::move(fields)),
        writer_index_(writer_index)
    {}

    void operator()(offer_context& ctx) {
        auto target = ctx.store_.ref();
        auto source = ctx.variables().store().ref();
        for(auto &f : fields_) {
            utils::copy_field(f.type_, target, f.target_offset_, source, f.source_offset_);
        }

        if (!ctx.writer_) {
            ctx.writer_ = ctx.task_context().downstream_writer(writer_index_);
        }
        ctx.writer_->write(target);
    }

    operator_kind kind() const noexcept override {
        return operator_kind::offer;
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

private:
    std::shared_ptr<meta::record_meta> meta_{};
    std::vector<details::offer_field> fields_{};
    std::size_t writer_index_{};

    std::shared_ptr<meta::record_meta> create_meta(
        processor_info const& info,
        meta::variable_order const& order,
        std::vector<column, takatori::util::object_allocator<column>> const& columns
    ) {
        std::vector<meta::field_type> fields{};
        auto sz = order.size();
        fields.resize(sz);
        for(auto&& c : columns) {
            fields[order.index(c.destination())] = utils::type_for(info.compiled_info(), c.destination());
        }
        return std::make_shared<meta::record_meta>(std::move(fields), boost::dynamic_bitset<std::uint64_t>(sz)); // TODO nullity
    }

    std::vector<details::offer_field> create_fields(
        std::shared_ptr<meta::record_meta> const& meta,
        meta::variable_order const& order,
        std::vector<column, takatori::util::object_allocator<column>> const& columns
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
                //TODO nullity
                false // nullable
            };
        }
        return fields;
    }
};

}


