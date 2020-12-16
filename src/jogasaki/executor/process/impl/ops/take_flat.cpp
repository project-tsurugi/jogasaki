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
#include "take_flat.h"

#include <vector>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/reader_container.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/validation.h>
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

ops::take_flat::take_flat(operator_base::operator_index_type index, const processor_info& info,
    operator_base::block_index_type block_index, const meta::variable_order& order,
    maybe_shared_ptr<meta::record_meta> meta, sequence_view<const column> columns, std::size_t reader_index,
    std::unique_ptr<operator_base> downstream) : record_operator(index, info, block_index),
    meta_(std::move(meta)),
    fields_(create_fields(meta_, order, columns)),
    reader_index_(reader_index),
    downstream_(std::move(downstream))
{
    utils::assert_all_fields_nullable(*meta_);
}

void ops::take_flat::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<take_flat_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<take_flat_context>(
            index(),
            ctx.block_scope(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    (*this)(*p, context);
}

void take_flat::operator()(take_flat_context& ctx, abstract::task_context* context) {
    auto target = ctx.variables().store().ref();
    if (! ctx.reader_) {
        auto r = ctx.task_context().reader(reader_index_);
        ctx.reader_ = r.reader<record_reader>();
    }
    auto resource = ctx.varlen_resource();
    while(ctx.reader_->next_record()) {
        utils::checkpoint_holder cp{resource};
        auto source = ctx.reader_->get_record();
        for(auto &f : fields_) {
            utils::copy_nullable_field(
                f.type_,
                target,
                f.target_offset_,
                f.target_nullity_offset_,
                source,
                f.source_offset_,
                f.source_nullity_offset_,
                resource
            );
        }
        if (downstream_) {
            unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
        }
    }
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

operator_kind take_flat::kind() const noexcept {
    return operator_kind::take_flat;
}

const maybe_shared_ptr<meta::record_meta>& take_flat::meta() const noexcept {
    return meta_;
}

void take_flat::finish(abstract::task_context*) {
    fail();
}

std::vector<details::take_flat_field>
take_flat::create_fields(const maybe_shared_ptr<meta::record_meta>& meta, const meta::variable_order& order,
    sequence_view<const column> columns) {
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
            meta->nullable(ind)
        };
    }
    return fields;
}

}


