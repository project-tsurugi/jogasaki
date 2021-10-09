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
#include "emit.h"

#include <vector>

#include <takatori/relation/emit.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/validation.h>
#include <jogasaki/executor/process/impl/ops/context_helper.h>
#include "operator_base.h"
#include "emit_context.h"

namespace jogasaki::executor::process::impl::ops {

emit::emit(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    sequence_view<const column> columns
) :
    record_operator(index, info, block_index),
    meta_(create_meta(info, columns)),
    fields_(create_fields(meta_, columns))
{
    utils::assert_all_fields_nullable(*meta_);
}

operation_status emit::process_record(abstract::task_context *context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<emit_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<emit_context>(
            index(),
            ctx.variable_table(block_index()),
            meta(),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p);
}

operation_status emit::operator()(emit_context &ctx) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    auto target = ctx.buffer_.ref();
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
    if (! ctx.writer_) {
        ctx.writer_ = ctx.task_context().external_writer(external_writer_index_);
    }
    ctx.writer_->write(target);
    return {};
}

const maybe_shared_ptr<meta::record_meta> &emit::meta() const noexcept {
    return meta_;
}

void emit::external_writer_index(std::size_t index) noexcept {
    external_writer_index_ = index;
}

std::shared_ptr<meta::record_meta> emit::create_meta(
    processor_info const& info,
    sequence_view<column const> columns
) {
    // FIXME currently respect the column order coming from takatori
    std::vector<meta::field_type> fields{};
    auto sz = columns.size();
    fields.reserve(sz);
    for(auto&& c : columns) {
        fields.emplace_back(utils::type_for(info.compiled_info(), c.source()));
    }
    return std::make_shared<meta::record_meta>(
        std::move(fields),
        boost::dynamic_bitset<std::uint64_t>(sz).flip()
    ); // assuming all fields nullable
}

std::vector<details::emit_field> emit::create_fields(
    maybe_shared_ptr<meta::record_meta> const& meta,
    sequence_view<column const> columns
) {
    std::vector<details::emit_field> fields{};
    std::size_t sz = meta->field_count();
    fields.reserve(sz);
    for(std::size_t ind = 0 ; ind < sz; ++ind) {
        auto&& c = columns[ind];
        auto& info = block_info().at(c.source());
        fields.emplace_back(details::emit_field{
            meta_->at(ind),
            info.value_offset(),
            meta_->value_offset(ind),
            info.nullity_offset(),
            meta_->nullity_offset(ind),
            true // assuming variables and output columns are all nullable
        });
    }
    return fields;
}

operator_kind emit::kind() const noexcept {
    return operator_kind::emit;
}

void emit::finish(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<emit_context>(index(), ctx.contexts());
    if (p && p->writer_) {
        p->writer_->flush();
    }
}

}


