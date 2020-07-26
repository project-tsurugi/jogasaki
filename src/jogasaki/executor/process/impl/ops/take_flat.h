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
#include "operator_base.h"
#include "take_flat_context.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

struct take_flat_field {
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
     */
    take_flat(
        processor_info const& info,
        block_index_type block_index,
        meta::variable_order const& order,
        std::vector<column, takatori::util::object_allocator<column>> const& columns,
        std::size_t reader_index
    ) : operator_base(info, block_index),
        meta_(create_meta(info, order, columns)),
        fields_(create_fields(meta_, order, columns)),
        reader_index_(reader_index)
    {}

    /**
     * @brief create new object
     */
    take_flat(
        processor_info const& info,
        block_index_type block_index,
        std::shared_ptr<meta::record_meta> meta,
        std::vector<details::take_flat_field> fields,
        std::size_t reader_index
    ) : operator_base(info, block_index),
        meta_(std::move(meta)),
        fields_(std::move(fields)),
        reader_index_(reader_index)
    {}

    bool operator()(take_flat_context& ctx) {
        auto target = ctx.variables().store().ref();
        if (! ctx.reader_) {
            auto r = ctx.task_context().reader(reader_index_);
            ctx.reader_ = r.reader<record_reader>();
        }
        if (ctx.reader_->next_record()) {
            auto source = ctx.reader_->get_record();
            for(auto &f : fields_) {
                utils::copy_field(f.type_, target, f.target_offset_, source, f.source_offset_);
            }
            return true;
        }
        return false;
    }

    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::take_flat;
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

private:
    std::shared_ptr<meta::record_meta> meta_{};
    std::vector<details::take_flat_field> fields_{};
    std::size_t reader_index_{};

    std::shared_ptr<meta::record_meta> create_meta(
        processor_info const& info,
        meta::variable_order const& order,
        std::vector<column, takatori::util::object_allocator<column>> const& columns
    ) {
        std::vector<meta::field_type> fields{};
        auto sz = order.size();
        fields.resize(sz);
        for(auto&& c : columns) {
            fields[order.index(c.source())] = utils::type_for(info.compiled_info(), c.source());
        }
        return std::make_shared<meta::record_meta>(std::move(fields), boost::dynamic_bitset<std::uint64_t>(sz)); // TODO nullity
    }

    std::vector<details::take_flat_field> create_fields(
        std::shared_ptr<meta::record_meta> const& meta,
        meta::variable_order const& order,
        std::vector<column, takatori::util::object_allocator<column>> const& columns
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


