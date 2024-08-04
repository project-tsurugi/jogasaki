/*
 * Copyright 2018-2023 Project Tsurugi.
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
#include "write_create.h"

#include <algorithm>
#include <functional>
#include <memory>
#include <stdexcept>
#include <tuple>
#include <type_traits>
#include <unordered_map>
#include <vector>
#include <boost/assert.hpp>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/variable.h>
#include <takatori/relation/write.h>
// #include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/optional_ptr.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/factory.h>
#include <yugawara/compiled_info.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/configuration.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/insert/fill_record_fields.h>
#include <jogasaki/executor/process/impl/expression/details/cast_evaluation.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/write_create_context.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/index/primary_context.h>
#include <jogasaki/index/primary_target.h>
#include <jogasaki/index/secondary_context.h>
#include <jogasaki/index/secondary_target.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/field_types.h>

#include "context_helper.h"
#include "details/error_abort.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using variable = takatori::descriptor::variable;
// using takatori::util::throw_exception;
using takatori::util::string_builder;

namespace details {

std::vector<index::secondary_target> create_secondary_targets(
    yugawara::storage::index const& idx
) {
    auto& table = idx.table();
    auto& primary = *table.owner()->find_primary_index(table);
    auto key_meta = index::create_meta(primary, true);
    auto value_meta = index::create_meta(primary, false);
    return insert::create_secondary_targets(idx, std::move(key_meta), std::move(value_meta));
}

}  // namespace details

void write_create::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if(auto* p = find_context<write_create_context>(index(), ctx.contexts())) {
        p->release();
    }
}

std::string_view write_create::storage_name() const noexcept {
    return entity_->primary().storage_name();
}

operator_kind write_create::kind() const noexcept {
    return operator_kind::write_create;
}

status fill_from_input_variables(
    insert::write_field const& write_field,
    index::field_info const& read_field,
    write_create_context& ctx,
    // compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    data::small_record_store& out
) {
    (void) resource;
    // auto& source_type = info.type_of(t.elements()[f.index_]);
    auto nocopy = nullptr;
    auto src = ctx.input_variables().store().ref();
    if (write_field.nullable_) { // assuming read field is always nullable
        utils::copy_nullable_field(
            read_field.type_,
            out.ref(),
            write_field.offset_,
            write_field.nullity_offset_,
            src,
            read_field.offset_,
            read_field.nullity_offset_,
            nocopy
        );
    } else {
        if (src.is_null(read_field.nullity_offset_)) {
            auto rc = status::err_integrity_constraint_violation;
            set_error(
                *ctx.req_context(),
                error_code::not_null_constraint_violation_exception,
                string_builder{} << "null assigned for non-nullable field." << string_builder::to_string,
                rc);
            return rc;
        }
        utils::copy_field(read_field.type_, out.ref(), write_field.offset_, src, read_field.offset_, nocopy);
    }

    // To clean up varlen data resource in data::any, we rely on upper layer that does clean up
    // on evey process invocation. Otherwise, we have to copy the result of conversion and
    // lifo resource is not convenient to copy the result when caller and callee use the same resource.

    /*
    data::any converted{res};
    if(conv::to_require_conversion(source_type, *f.target_type_)) {
        if(auto st = conv::conduct_assignment_conversion(
            source_type,
            *f.target_type_,
            res,
            converted,
            ctx,
            std::addressof(resource)
        );
        st != status::ok) {
            return st;
        }
    }
    */

    return status::ok;
}

status create_record_from_input_variables(
    write_create_context& ctx,
    std::vector<insert::write_field> const& write_fields,
    std::vector<index::field_info> const& read_fields,
    // compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    data::small_record_store& out
) {
    BOOST_ASSERT(write_fields.size() == read_fields.size());  //NOLINT
    for(std::size_t i=0, n=write_fields.size(); i<n; ++i) {
        auto& wf = write_fields[i];
        auto& rf = read_fields[i];
        if (wf.index_ == insert::npos) {
            // value not specified for the field use default value or null
            if(auto res = insert::fill_default_value(wf, *ctx.req_context(), resource, out); res != status::ok) {
                return res;
            }
            continue;
        }
        if(auto res = fill_from_input_variables(wf, rf, ctx, resource, out); res != status::ok) {
            return res;
        }
    }
    return status::ok;
}

operation_status write_create::operator()(write_create_context& ctx) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }

    insert::write_context wctx(
        *ctx.req_context(),
        storage_name(),
        entity_->primary().key_meta(),
        entity_->primary().value_meta(),
        entity_->secondaries(),
        *ctx.req_context()->database(),
        ctx.varlen_resource()
    );  // currently common::write uses the same resource for building mirror and executing runtime

    // create record from input variables

    utils::checkpoint_holder cph(ctx.varlen_resource());

    if(auto res = create_record_from_input_variables(
           ctx,
           key_fields_to_write_,
           key_fields_to_read_,
           *ctx.varlen_resource(),
           wctx.key_store_
       );
       res != status::ok) {
        return {operation_status_kind::aborted};
    }
    if(auto res = create_record_from_input_variables(
           ctx,
           value_fields_to_write_,
           value_fields_to_read_,
           *ctx.varlen_resource(),
           wctx.value_store_
       );
       res != status::ok) {
        return {operation_status_kind::aborted};
    }

    if(! entity_->process_record(*ctx.req_context(), wctx)) {
        return {operation_status_kind::aborted};
    }
    return {};
}

operation_status write_create::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<write_create_context>(index(), ctx.contexts());
    if (! p) {
        std::vector<index::secondary_context> contexts{};
        contexts.reserve(entity_->secondaries().size());
        for(auto&& s : entity_->secondaries()) {
            contexts.emplace_back(
                ctx.database()->get_or_create_storage(s.storage_name()),
                ctx.req_context()
            );
        }
        p = ctx.make_context<write_create_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(storage_name()),
            ctx.transaction(),
            entity_->primary().key_meta(),
            entity_->primary().value_meta(),
            ctx.resource(),
            ctx.varlen_resource(),
            std::move(contexts)
        );
    }
    return (*this)(*p);
}

write_create::write_create(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    yugawara::storage::index const& idx,
    sequence_view<takatori::relation::details::mapping_element const> columns,
    memory::lifo_paged_memory_resource* resource,
    variable_table_info const* input_variable_info
) :
    record_operator(index, info, block_index, input_variable_info),
    kind_(kind),
    key_meta_(index::create_meta(idx, true)),
    value_meta_(index::create_meta(idx, false)),
    key_fields_to_write_(insert::create_fields(idx, columns, key_meta_, value_meta_, true, resource)),
    value_fields_to_write_(insert::create_fields(idx, columns, key_meta_, value_meta_, false, resource)),
    key_fields_to_read_(index::create_fields(idx, columns, (input_variable_info != nullptr ? *input_variable_info : info.vars_info_list()[block_index]), true, false)),
    value_fields_to_read_(index::create_fields(idx, columns, (input_variable_info != nullptr ? *input_variable_info : info.vars_info_list()[block_index]), false, false)),
    entity_(std::make_shared<insert::insert_new_record>(
        kind_,
        insert::create_primary_target(idx.simple_name(), key_meta_, value_meta_, key_fields_to_write_, value_fields_to_write_),
        insert::create_secondary_targets(idx, key_meta_, value_meta_)
    ))
{}

index::primary_target const& write_create::primary() const noexcept {
    return entity_->primary();
}

}  // namespace jogasaki::executor::process::impl::ops
