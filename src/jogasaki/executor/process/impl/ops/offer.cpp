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

#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/conv/require_conversion.h>
#include <jogasaki/executor/conv/unify.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/data/field_operations.h>
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
    assert_with_exception(context != nullptr, context);
    context_helper ctx{*context};
    auto* p = ctx.find_context<offer_context>(index());
    if (! p) {
        p = ctx.make_context<offer_context>(
            index(),
            block_index(),
            meta(),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p);
}

operation_status offer::operator()(offer_context& ctx) {
    if (ctx.aborted()) {
        return operation_status_kind::aborted;
    }
    auto target = ctx.store_.ref();
    utils::checkpoint_holder h{ctx.varlen_resource()};
    for(auto &f : fields_) {
        auto source = ctx.variables().ref(f.source_region_id_);
        if (auto result = f.assignment_.apply(source, target, ctx.req_context(), ctx.varlen_resource());
            result != status::ok) {
            if (ctx.req_context()) {
                set_error_context(*ctx.req_context(), error_code::sql_execution_exception,
                    "unexpected error in type conversion", status::err_io_error);
            }
            ctx.abort();
            return operation_status_kind::aborted;
        }
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
        return operation_status_kind::aborted;
    }
    return operation_status_kind::ok;
}

operator_kind offer::kind() const noexcept {
    return operator_kind::offer;
}

const maybe_shared_ptr<meta::record_meta>& offer::meta() const noexcept {
    return meta_;
}

void offer::finish(abstract::task_context* context) {
    assert_with_exception(context != nullptr, context);
    context->deactivate_writer(writer_index_);
    context_helper ctx{*context};
    if (auto* p = ctx.find_context<offer_context>(index())) {
        p->release();
    }
}

std::vector<details::offer_field> offer::create_fields(
    maybe_shared_ptr<meta::record_meta> const& meta,
    meta::variable_order const& order,
    sequence_view<const column> columns,
    processor_info const& pinfo
) {
    std::vector<std::optional<details::offer_field>> prepared(meta->field_count());
    for (auto const& c : columns) {
        auto index = order.index(c.destination());
        auto& location = block_info().at(c.source());
        auto const& source_type = pinfo.compiled_info().type_of(c.source());
        auto const& target_type = pinfo.compiled_info().type_of(c.destination());
        assert_with_exception(!prepared.at(index).has_value(), index);
        prepared[index].emplace(details::offer_field{
            conv::record_assignment::prepare_record(source_type, target_type, utils::type_for(source_type),
                {location.value_offset(), location.nullity_offset()}, meta->at(index),
                {meta->value_offset(index), meta->nullity_offset(index)}, meta->nullable(index),
                conv::conversion_kind::unify), location.region()});
    }
    std::vector<details::offer_field> fields;
    fields.reserve(prepared.size());
    for (auto& field : prepared) {
        assert_with_exception(field.has_value());
        fields.emplace_back(*field);
    }
    return fields;
}

}  // namespace jogasaki::executor::process::impl::ops
