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
#include "offer.h"

#include <vector>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/validation.h>
#include "operator_base.h"
#include "offer_context.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

offer::offer(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    meta::variable_order const& order,
    maybe_shared_ptr<meta::record_meta> meta,
    sequence_view<column const> columns,
    std::size_t writer_index
) : record_operator(index, info, block_index),
    meta_(std::move(meta)),
    fields_(create_fields(meta_, order, columns)),
    writer_index_(writer_index)
{
    utils::assert_all_fields_nullable(*meta_);
}

operation_status offer::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<offer_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<offer_context>(
            index(),
            meta(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p);
}

operation_status offer::operator()(offer_context& ctx) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    auto target = ctx.store_.ref();
    auto source = ctx.input_variables().store().ref();
    for(auto &f : fields_) {
        utils::copy_nullable_field(
            f.type_,
            target,
            f.target_offset_,
            f.target_nullity_offset_,
            source,
            f.source_offset_,
            f.source_nullity_offset_
        );
    }

    if (!ctx.writer_) {
        ctx.writer_ = ctx.task_context().downstream_writer(writer_index_);
    }
    ctx.writer_->write(target);
    return {};
}

operator_kind offer::kind() const noexcept {
    return operator_kind::offer;
}

const maybe_shared_ptr<meta::record_meta>& offer::meta() const noexcept {
    return meta_;
}

void offer::finish(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    if (auto* p = find_context<offer_context>(index(), ctx.contexts())) {
        p->release();
    }
}

std::vector<details::offer_field> offer::create_fields(
    maybe_shared_ptr<meta::record_meta> const& meta,
    meta::variable_order const& order,
    sequence_view<const column> columns
) {
    std::vector<details::offer_field> fields{};
    fields.resize(meta->field_count());
    for(auto&& c : columns) {
        auto ind = order.index(c.destination());
        auto& info = block_info().at(c.source());
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

}
