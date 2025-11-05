/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>
#include <vector>
#include <boost/assert.hpp>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>

#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>

#include <jogasaki/constants.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/io/record_writer.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/context_helper.h>
#include <jogasaki/executor/process/impl/ops/details/error_abort.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/string_manipulation.h>
#include <jogasaki/utils/validation.h>

#include "emit_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

emit::emit(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    sequence_view<const column> columns
) :
    record_operator(index, info, block_index),
    meta_(create_meta(info.compiled_info(), columns)),
    fields_(create_fields(meta_->origin(), columns))
{
    utils::assert_all_fields_nullable(*meta_->origin());
}

operation_status emit::process_record(abstract::task_context *context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<emit_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<emit_context>(
            index(),
            ctx.variable_table(block_index()),
            meta()->origin(),
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
    auto* writer = ctx.task_context().external_writer();
    if(! writer) {
        // writer must have been reserved at the beginning of this process task, so normally this should not happen
        set_error(
            *ctx.req_context(),
            error_code::sql_execution_exception,
            "failed to acquire writer",
            status::err_io_error
        );
        ctx.abort();
        return {operation_status_kind::aborted};
    }
    if(! writer->write(target)) {
        // possibly writer error due to buffer overflow
        // TODO retrieve the exact reason from writer and construct error message based on it
        set_error(
            *ctx.req_context(),
            error_code::sql_execution_exception,
            "an error occurred in writing output records, possibly due to buffer overflow in endpoint",
            status::err_io_error
        );
        ctx.abort();
        return {operation_status_kind::aborted};
    }
    return {};
}

maybe_shared_ptr<meta::external_record_meta> const& emit::meta() const noexcept {
    return meta_;
}

std::shared_ptr<meta::external_record_meta> emit::create_meta(
    yugawara::compiled_info const& info,
    sequence_view<const column> columns
) {
    std::vector<std::optional<std::string>> field_names{};
    std::vector<meta::field_type> fields{};
    auto sz = columns.size();
    fields.reserve(sz);
    field_names.reserve(sz);
    for(auto&& c : columns) {
        // remove the generated pk column. It should be invisible to client.
        if(c.name() && utils::is_prefix(*c.name(), generated_pkey_column_prefix)) {
            --sz;
            continue;
        }
        fields.emplace_back(utils::type_for(info, c.source()));
        // c.name() can accidentally return empty string - fall back to nulloopt then. TODO remove if takatori is fixed
        field_names.emplace_back(c.name() && c.name()->empty() ? std::nullopt : c.name());
    }
    return std::make_shared<meta::external_record_meta>(
        std::make_shared<meta::record_meta>(
            std::move(fields),
            boost::dynamic_bitset<std::uint64_t>(sz).flip()
        ),
        std::move(field_names)
    ); // assuming all fields nullable
}

std::vector<details::emit_field> emit::create_fields(
    maybe_shared_ptr<meta::record_meta> const& meta,
    sequence_view<column const> columns
) {
    std::vector<details::emit_field> fields{};
    std::size_t sz = meta->field_count();
    fields.reserve(sz);
    std::size_t pos = 0; // field position in meta
    for(auto&& c : columns) {
        if(c.name() && utils::is_prefix(*c.name(), generated_pkey_column_prefix)) {
            continue;
        }
        auto& info = block_info().at(c.source());
        fields.emplace_back(details::emit_field{
            meta_->at(pos),
            info.value_offset(),
            meta_->value_offset(pos),
            info.nullity_offset(),
            meta_->nullity_offset(pos),
            true // assuming variables and output columns are all nullable
        });
        ++pos;
    }
    return fields;
}

operator_kind emit::kind() const noexcept {
    return operator_kind::emit;
}

void emit::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if (auto* p = find_context<emit_context>(index(), ctx.contexts())) {
        p->release();
    }
}

}


