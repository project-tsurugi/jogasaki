/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <type_traits>
#include <utility>
#include <vector>
#include <boost/assert.hpp>

#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/conv/require_conversion.h>
#include <jogasaki/executor/conv/unify.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/validation.h>

#include "context_helper.h"
#include "offer_context.h"
#include "operator_base.h"

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
    fields_(create_fields(meta_, order, columns, info)),
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
    utils::checkpoint_holder h{ctx.varlen_resource()};
    for(auto &f : fields_) {
        if(! f.requires_conversion_) {
            utils::copy_nullable_field(
                f.type_,
                target,
                f.target_offset_,
                f.target_nullity_offset_,
                source,
                f.source_offset_,
                f.source_nullity_offset_
            );
            continue;
        }
        data::any src{};
        // varlen fields data is already on `resource`, so no need to copy
        auto nocopy = nullptr;
        utils::copy_nullable_field_as_any(
            f.source_ftype_,
            source,
            f.source_offset_,
            f.source_nullity_offset_,
            src,
            nocopy
        );

        // rewinding varlen_resource is done by the checkpoint_holder above
        data::any converted{};
        if(auto res = conv::conduct_unifying_conversion(
               *f.source_type_,
               *f.target_type_,
               src,
               converted,
               ctx.varlen_resource()
           );
           res != status::ok) {
            // normaly unifying conversion does not fail
            set_error_context(
                *ctx.req_context(),
                error_code::sql_execution_exception,
                "unexpected error in type conversion",
                status::err_io_error
            );
            ctx.abort();
            return {operation_status_kind::aborted};
        }
        utils::copy_nullable_field(f.type_, target, f.target_offset_, f.target_nullity_offset_, converted, nocopy);
    }

    if (! ctx.writer_) {
        ctx.writer_ = ctx.task_context().downstream_writer(writer_index_);
    }
    if(! ctx.writer_->write(target)) {
        set_error_context(
            *ctx.req_context(),
            error_code::sql_execution_exception,
            "unexpected error when writing data to exchange",
            status::err_io_error
        );
        ctx.abort();
        return {operation_status_kind::aborted};
    }
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
    context->deactivate_writer(writer_index_);
    context_helper ctx{*context};
    if (auto* p = find_context<offer_context>(index(), ctx.contexts())) {
        p->release();
    }
}

std::vector<details::offer_field> offer::create_fields(
    maybe_shared_ptr<meta::record_meta> const& meta,
    meta::variable_order const& order,
    sequence_view<const column> columns,
    processor_info const& pinfo
) {
    std::vector<details::offer_field> fields{};
    fields.resize(meta->field_count());
    for(auto&& c : columns) {
        auto ind = order.index(c.destination());
        auto& info = block_info().at(c.source());
        auto const& source_type = pinfo.compiled_info().type_of(c.source());
        auto source_ftype = utils::type_for(source_type);
        auto const& target_type = pinfo.compiled_info().type_of(c.destination());
        fields[ind] = details::offer_field{
            meta->at(ind),
            source_ftype,
            info.value_offset(),
            meta->value_offset(ind),
            info.nullity_offset(),
            meta->nullity_offset(ind),
            meta->nullable(ind),
            std::addressof(source_type),
            std::addressof(target_type),
            conv::to_require_conversion(source_type, target_type)
        };
    }
    return fields;
}

}  // namespace jogasaki::executor::process::impl::ops
