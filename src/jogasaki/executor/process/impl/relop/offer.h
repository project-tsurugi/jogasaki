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
#include <jogasaki/executor/process/impl/block_variables.h>
#include <jogasaki/utils/copy_field_data.h>
#include "operator_base.h"
#include "offer_context.h"

namespace jogasaki::executor::process::impl::relop {

using column = takatori::relation::step::offer::column;

namespace details {

struct field {
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
    /**
     * @brief create empty object
     */
    offer() = default;

    /**
     * @brief create new object
     */
    explicit offer(
        std::shared_ptr<meta::record_meta> meta,
        exchange::step const* target,
        std::vector<column, takatori::util::object_allocator<column>> const& columns,
        std::vector<block_variables_info> const& blocks
    ) :
        meta_(std::move(meta)),
        target_(target)
    {
        auto& order = target->column_order();
        fields_.resize(meta->field_count());
        for(auto&& c : columns) {
            auto ind = order.index(c.destination());
            fields_[ind] = details::field{
                meta->at(ind),
                blocks[block_index()].value_map().at(c.source()).value_offset(),
                meta->value_offset(ind),
                0, // src nullity offset
                0, // tgt nullity offset
                false // nullable
            };
        }
    }

    void operator()(offer_context& ctx) {
        auto target = ctx.store_.ref();
        auto source = ctx.variables().store().ref();
        for(auto &f : fields_) {
            utils::copy_field(f.type_, target, f.target_offset_, source, f.source_offset_);
        }

        if (ctx.writer_) {
            // FIXME fetch writer when needed
            ctx.writer_->write(target);
        }
    }

    operator_kind kind() override {
        return operator_kind::offer;
    }

    [[nodiscard]] std::shared_ptr<meta::record_meta> const& meta() const noexcept {
        return meta_;
    }

private:
    std::shared_ptr<meta::record_meta> meta_{};
    exchange::step const* target_{};
    variable_value_map map_{};
    std::vector<details::field> fields_{};
};

}


