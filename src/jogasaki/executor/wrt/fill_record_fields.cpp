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
#include "fill_record_fields.h"

#include <cstddef>
#include <string_view>
#include <utility>
#include <vector>

#include <takatori/descriptor/variable.h>
#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>
#include <yugawara/storage/index.h>

#include <jogasaki/common_types.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/common/step.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/conv/create_default_value.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/ops/default_value_kind.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/executor/wrt/insert_new_record.h>
#include <jogasaki/executor/wrt/write_field.h>
#include <jogasaki/index/primary_context.h>
#include <jogasaki/index/primary_target.h>
#include <jogasaki/index/secondary_context.h>
#include <jogasaki/index/secondary_target.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/model/statement.h>
#include <jogasaki/model/statement_kind.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/handle_encode_errors.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/make_function_context.h>
#include <jogasaki/utils/validate_any_type.h>

namespace jogasaki::executor::wrt {

using takatori::util::string_builder;
using takatori::util::throw_exception;

status next_sequence_value(request_context& ctx, sequence_definition_id def_id, sequence_value& out) {
    BOOST_ASSERT(ctx.sequence_manager() != nullptr); //NOLINT
    auto& mgr = *ctx.sequence_manager();
    auto* seq = mgr.find_sequence(def_id);
    if(seq == nullptr) {
        throw_exception(std::logic_error{""});
    }
    auto ret = seq->next(*ctx.transaction()->object());
    // even if there is an error with next(), it mark the sequence as used by the tx, so call notify_updates first
    // in order to flush the in-flight updates, otherwise the sequence marked as used will leak and cause crash
    // when the other tx uses the same address and the leaked sequence pointer is re-used
    try {
        mgr.notify_updates(*ctx.transaction()->object());
    } catch(executor::sequence::exception const& e) {
        return e.get_status();
    }
    if(! ret) {
        auto rc = status::err_illegal_operation;
        auto min_or_max = ret.error() == sequence::sequence_error::out_of_upper_bound ? "maximum" : "minimum";
        set_error(
            ctx,
            error_code::value_evaluation_exception,
            string_builder{} << "reached " << min_or_max << " value of sequence:" << seq->info().name() << string_builder::to_string,
            rc);
        return rc;
    }
    out = ret.value();
    return status::ok;
}

status assign_value_to_field(
    wrt::write_field const& f,
    data::any src,
    request_context& ctx,
    memory::lifo_paged_memory_resource& resource,
    data::small_record_store& out
) {
    auto is_null = src.empty();
    if (is_null && !f.nullable_) {
        auto rc = status::err_integrity_constraint_violation;
        set_error(
            ctx,
            error_code::not_null_constraint_violation_exception,
            string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
            rc);
        return rc;
    }
    out.ref().set_null(f.nullity_offset_, is_null);
    if (f.nullable_) {
        utils::copy_nullable_field(
            f.type_,
            out.ref(),
            f.offset_,
            f.nullity_offset_,
            src,
            std::addressof(resource)
        );
    } else {
        utils::copy_field(f.type_, out.ref(), f.offset_, src, std::addressof(resource));
    }
    return status::ok;
}

status fill_default_value(
    wrt::write_field const& f,
    request_context& ctx,
    memory::lifo_paged_memory_resource& resource,
    data::small_record_store& out
) {
    switch (f.kind_) {
        case process::impl::ops::default_value_kind::nothing:
            if (!f.nullable_) {
                set_error(
                    ctx,
                    error_code::not_null_constraint_violation_exception,
                    string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
                    status::err_integrity_constraint_violation);
                return status::err_integrity_constraint_violation;
            }
            out.ref().set_null(f.nullity_offset_, true);
            break;
        case process::impl::ops::default_value_kind::immediate: {
            auto src = f.immediate_value_;
            assign_value_to_field(f, src, ctx, resource, out);
            break;
        }
        case process::impl::ops::default_value_kind::sequence: {
            // increment sequence - loop might increment the sequence twice
            sequence_value v{};
            if (auto res = next_sequence_value(ctx, f.def_id_, v); res != status::ok) {
                handle_encode_errors(ctx, res);
                handle_generic_error(ctx, res, error_code::sql_service_exception);
                return res;
            }
            out.ref().set_null(f.nullity_offset_, false);
            out.ref().set_value<std::int64_t>(f.offset_, v);
            break;
        }
        case process::impl::ops::default_value_kind::function: {
            process::impl::expression::evaluator_context c{
                std::addressof(resource),
                utils::make_function_context(*ctx.transaction())
            };
            auto src = f.function_(c);
            // TODO validate_any_type cannot detect the type difference
            // such as time_point_type(true) and time_point_type(false)
            if(! utils::validate_any_type(src, f.type_)) {
                auto rc = status::err_unsupported;
                set_error(
                    ctx,
                    error_code::invalid_runtime_value_exception,
                    string_builder{} << "invalid value was assigned as default value field-type:" << f.type_
                                     << " value-index:" << src.type_index() << string_builder::to_string,
                    rc
                );
                return rc;
            }
            assign_value_to_field(f, src, ctx, resource, out);
            break;
        }
    }
    return status::ok;
}

void create_generated_field(
    std::vector<wrt::write_field>& ret,
    std::size_t index,
    yugawara::storage::column_value const& dv,
    takatori::type::data const& type,
    bool nullable,
    kvs::coding_spec spec,
    std::size_t offset,
    std::size_t nullity_offset,
    memory::lifo_paged_memory_resource* resource
) {
    using yugawara::storage::column_value_kind;
    sequence_definition_id def_id{};
    data::aligned_buffer buf{};
    auto knd = process::impl::ops::default_value_kind::nothing;
    data::any immediate_val{};
    yugawara::function::configurable_provider const* functions = nullptr;
    switch(dv.kind()) {
        case column_value_kind::nothing:
            break;
        case column_value_kind::immediate: {
            knd = process::impl::ops::default_value_kind::immediate;
            auto& v = *dv.element<column_value_kind::immediate>();
            if(auto a = conv::create_immediate_default_value(v, type, resource); ! a.error()) {
                immediate_val = a; // varlen resource of the any content is owned by the executable_statement
                break;
            }
            // the value must have been validated when ddl is issued
            fail_with_exception();
        }
        case column_value_kind::sequence: {
            knd = process::impl::ops::default_value_kind::sequence;
            if (auto id = dv.element<column_value_kind::sequence>()->definition_id()) {
                def_id = *id;
            } else {
                throw_exception(std::logic_error{"sequence must be defined with definition_id"});
            }
            break;
        }
        case column_value_kind::function: {
            knd = process::impl::ops::default_value_kind::function;
            if (auto id = dv.element<column_value_kind::function>()->definition_id()) {
                def_id = id;
            } else {
                throw_exception(std::logic_error{"function must be defined with definition_id"});
            }
            functions = global::scalar_function_provider().get();
            break;
        }
    }
    ret.emplace_back(
        index,
        type,
        spec,
        nullable,
        offset,
        nullity_offset,
        knd,
        immediate_val,
        def_id,
        functions
    );
}

std::vector<wrt::write_field> create_fields(
    yugawara::storage::index const& idx,
    sequence_view<takatori::relation::details::mapping_element const> columns,
    maybe_shared_ptr<meta::record_meta> key_meta,  //NOLINT(performance-unnecessary-value-param)
    maybe_shared_ptr<meta::record_meta> value_meta,  //NOLINT(performance-unnecessary-value-param)
    bool key,
    memory::lifo_paged_memory_resource* resource
) {
    std::vector<takatori::descriptor::variable> destination{};
    destination.reserve(columns.size());
    for(auto&& c : columns) {
        destination.emplace_back(c.destination());
    }
    return create_fields(idx, destination, std::move(key_meta), std::move(value_meta), key, resource);
}

std::vector<wrt::write_field> create_fields(
    yugawara::storage::index const& idx,
    sequence_view<takatori::descriptor::variable const> columns,
    maybe_shared_ptr<meta::record_meta> key_meta,  //NOLINT(performance-unnecessary-value-param)
    maybe_shared_ptr<meta::record_meta> value_meta,  //NOLINT(performance-unnecessary-value-param)
    bool key,
    memory::lifo_paged_memory_resource* resource
) {
    using reference = takatori::descriptor::variable::reference_type;
    yugawara::binding::factory bindings{};
    std::vector<wrt::write_field> out{};
    std::unordered_map<reference, std::size_t> variable_indices{};
    for(std::size_t i=0, n=columns.size(); i<n; ++i) {
        auto&& c = columns[i];
        variable_indices[c.reference()] = i;
    }
    if (key) {
        out.reserve(idx.keys().size());
        for(auto&& k : idx.keys()) {
            auto kc = bindings(k.column());
            auto& type = k.column().type();
            auto spec = k.direction() == takatori::relation::sort_direction::ascendant ?
                kvs::spec_key_ascending : kvs::spec_key_descending;
            bool nullable = k.column().criteria().nullity().nullable();
            if(variable_indices.count(kc.reference()) == 0) {
                // no column specified - use default value
                auto& dv = k.column().default_value();
                auto pos = out.size();
                create_generated_field(
                    out,
                    npos,
                    dv,
                    type,
                    nullable,
                    spec,
                    key_meta->value_offset(pos),
                    key_meta->nullity_offset(pos),
                    resource
                );
                continue;
            }
            if(k.column().features().contains(::yugawara::storage::column_feature::read_only)) {
                auto msg = string_builder{}
                    << "write operation on read-only column name:" << k.column().simple_name()
                    << string_builder::to_string;
                throw_exception(plan::impl::compile_exception{
                    create_error_info(error_code::restricted_operation_exception, msg, status::err_illegal_operation)
                });
            }
            auto pos = out.size();
            out.emplace_back(
                variable_indices[kc.reference()],
                type,
                spec,
                nullable,
                key_meta->value_offset(pos),
                key_meta->nullity_offset(pos)
            );
        }
    } else {
        out.reserve(idx.values().size());
        for(auto&& v : idx.values()) {
            auto b = bindings(v);

            auto& c = static_cast<yugawara::storage::column const&>(v);
            auto& type = c.type();
            bool nullable = c.criteria().nullity().nullable();
            auto spec = kvs::spec_value;
            if(variable_indices.count(b.reference()) == 0) {
                // no column specified - use default value
                auto& dv = c.default_value();
                auto pos = out.size();
                create_generated_field(
                    out,
                    npos,
                    dv,
                    type,
                    nullable,
                    spec,
                    value_meta->value_offset(pos),
                    value_meta->nullity_offset(pos),
                    resource
                );
                continue;
            }
            if(c.features().contains(::yugawara::storage::column_feature::read_only)) {
                auto msg = string_builder{} << "write operation on read-only column name:" << c.simple_name()
                                            << string_builder::to_string;
                throw_exception(plan::impl::compile_exception{
                    create_error_info(error_code::restricted_operation_exception, msg, status::err_illegal_operation)
                });
            }
            auto pos = out.size();
            out.emplace_back(
                variable_indices[b.reference()],
                type,
                spec,
                nullable,
                value_meta->value_offset(pos),
                value_meta->nullity_offset(pos)
            );
        }
    }
    return out;
}

primary_target create_primary_target(
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    std::vector<wrt::write_field> const& key_fields,
    std::vector<wrt::write_field> const& value_fields
) {
    std::vector<index::field_info> input_key_fields{};
    input_key_fields.reserve(key_fields.size());
    for(auto&& f : key_fields) {
        input_key_fields.emplace_back(
            f.type_,
            true,
            f.offset_,
            f.nullity_offset_,
            f.nullable_,
            f.spec_
        );
    }
    std::vector<index::field_info> input_value_fields{};
    input_value_fields.reserve(value_fields.size());
    for(auto&& f : value_fields) {
        input_value_fields.emplace_back(
            f.type_,
            true,
            f.offset_,
            f.nullity_offset_,
            f.nullable_,
            f.spec_
        );
    }
    return {
        storage_name,
        std::move(key_meta),
        std::move(value_meta),
        input_key_fields,
        input_key_fields,
        std::move(input_value_fields)
    };
}

std::vector<secondary_target> create_secondary_targets(
    yugawara::storage::index const& idx,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta
) {
    std::vector<secondary_target> ret{};
    auto cnt = 0;
    idx.table().owner()->each_table_index(idx.table(),
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (*entry == idx) {
                return;
            }
            ++cnt;
        }
    );
    ret.reserve(cnt);
    idx.table().owner()->each_table_index(idx.table(),
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (*entry == idx) {
                return;
            }
            ret.emplace_back(
                *entry,
                key_meta,
                value_meta
            );
        }
    );
    return ret;
}


}  // namespace jogasaki::executor::wrt
