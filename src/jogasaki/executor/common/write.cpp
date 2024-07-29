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
#include "write.h"

#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <optional>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <boost/assert.hpp>

#include <takatori/descriptor/element.h>
#include <takatori/descriptor/reference.h>
#include <takatori/descriptor/variable.h>
#include <takatori/relation/sort_direction.h>
#include <takatori/tree/tree_element_vector.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/type/data.h>
#include <takatori/util/exception.h>
#include <takatori/util/optional_ptr.h>
#include <takatori/util/sequence_view.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/factory.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/column_value.h>
#include <yugawara/storage/column_value_kind.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>
#include <yugawara/variable/criteria.h>
#include <yugawara/variable/nullity.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/data/any.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/conv/create_default_value.h>
#include <jogasaki/executor/process/impl/expression/error.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/default_value_kind.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/index/secondary_target.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/abort_transaction.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/handle_encode_errors.h>
#include <jogasaki/utils/handle_generic_error.h>

namespace jogasaki::executor::common {

using jogasaki::executor::process::impl::ops::write_kind;
using jogasaki::executor::process::impl::expression::evaluator;

using takatori::util::throw_exception;
using takatori::util::string_builder;

constexpr static std::size_t npos = static_cast<std::size_t>(-1);

status next_sequence_value(request_context& ctx, sequence_definition_id def_id, sequence_value& out) {
    BOOST_ASSERT(ctx.sequence_manager() != nullptr); //NOLINT
    auto& mgr = *ctx.sequence_manager();
    auto* seq = mgr.find_sequence(def_id);
    if(seq == nullptr) {
        throw_exception(std::logic_error{""});
    }
    auto ret = seq->next(*ctx.transaction()->object());
    try {
        mgr.notify_updates(*ctx.transaction()->object());
    } catch(executor::sequence::exception const& e) {
        return e.get_status();
    }
    out = ret;
    return status::ok;
}

status fill_default_value(
    details::write_field const& f,
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
    }
    return status::ok;
}


status fill_evaluated_value(
    details::write_field const& f,
    request_context& ctx,
    write::tuple const& t,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables,
    data::small_record_store& out
) {
    auto& source_type = info.type_of(t.elements()[f.index_]);
    evaluator eval{t.elements()[f.index_], info, host_variables};
    process::impl::variable_table empty{};
    process::impl::expression::evaluator_context c{std::addressof(resource)};
    auto res = eval(c, empty, std::addressof(resource));
    if (res.error()) {
        auto err = res.to<process::impl::expression::error>();
        if(err.kind() == process::impl::expression::error_kind::lost_precision_value_too_long) {
            auto rc = status::err_expression_evaluation_failure;
            set_error(
                ctx,
                error_code::value_too_long_exception,
                "evaluated value was too long to write",
                rc
            );
            return rc;
        }
        auto rc = status::err_expression_evaluation_failure;
        set_error(
            ctx,
            error_code::value_evaluation_exception,
            string_builder{} << "An error occurred in evaluating values. error:"
                             << res.to<process::impl::expression::error>() << string_builder::to_string,
            rc
        );
        return rc;
    }

    // To clean up varlen data resource in data::any, we rely on upper layer that does clean up
    // on evey process invocation. Otherwise, we have to copy the result of conversion and
    // lifo resource is not convenient to copy the result when caller and callee use the same resource.
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
    // varlen fields data is already on `resource`, so no need to copy
    auto nocopy = nullptr;
    if (f.nullable_) {
        utils::copy_nullable_field(f.type_, out.ref(), f.offset_, f.nullity_offset_, converted, nocopy);
    } else {
        if (!converted) {
            auto rc = status::err_integrity_constraint_violation;
            set_error(
                ctx,
                error_code::not_null_constraint_violation_exception,
                string_builder{} << "Null assigned for non-nullable field." << string_builder::to_string,
                rc);
            return rc;
        }
        utils::copy_field(f.type_, out.ref(), f.offset_, converted, nocopy);
    }
    return status::ok;
}

status create_record_from_tuple(  //NOLINT(readability-function-cognitive-complexity)
    request_context& ctx,
    write::tuple const& t,
    std::vector<details::write_field> const& fields,
    compiled_info const& info,
    memory::lifo_paged_memory_resource& resource,
    executor::process::impl::variable_table const* host_variables,
    data::small_record_store& out
) {
    for (auto&& f: fields) {
        if (f.index_ == npos) {
            // value not specified for the field use default value or null
            if(auto res = fill_default_value(f, ctx, resource, out); res != status::ok) {
                return res;
            }
            continue;
        }
        if(auto res = fill_evaluated_value(f, ctx, t, info, resource, host_variables, out); res != status::ok) {
            return res;
        }
    }
    return status::ok;
}

void create_generated_field(
    std::vector<details::write_field>& ret,
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
            throw_exception(std::logic_error{"function default value is unsupported now"});
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
        def_id
    );
}

std::vector<details::write_field> create_fields(
    yugawara::storage::index const& idx,
    sequence_view<write::column const> columns,
    maybe_shared_ptr<meta::record_meta> key_meta,  //NOLINT(performance-unnecessary-value-param)
    maybe_shared_ptr<meta::record_meta> value_meta,  //NOLINT(performance-unnecessary-value-param)
    bool key,
    memory::lifo_paged_memory_resource* resource
) {
    using reference = takatori::descriptor::variable::reference_type;
    yugawara::binding::factory bindings{};
    std::vector<details::write_field> out{};
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
    std::vector<details::write_field> const& key_fields,
    std::vector<details::write_field> const& value_fields
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

write::write(
    write_kind kind,
    yugawara::storage::index const& idx,
    takatori::statement::write const& wrt,
    memory::lifo_paged_memory_resource& resource,
    compiled_info info,
    executor::process::impl::variable_table const* host_variables
) :
    kind_(kind),
    idx_(std::addressof(idx)),
    wrt_(std::addressof(wrt)),
    resource_(std::addressof(resource)),
    info_(std::move(info)),
    host_variables_(host_variables),
    key_meta_(index::create_meta(*idx_, true)),
    value_meta_(index::create_meta(*idx_, false)),
    key_fields_(create_fields(*idx_, wrt_->columns(), key_meta_, value_meta_, true, resource_)),
    value_fields_(create_fields(*idx_, wrt_->columns(), key_meta_, value_meta_, false, resource_)),
    primary_(create_primary_target(idx_->simple_name(), key_meta_, value_meta_, key_fields_, value_fields_)),
    secondaries_(create_secondary_targets(*idx_, key_meta_, value_meta_))
{}

model::statement_kind write::kind() const noexcept {
    return model::statement_kind::write;
}

bool write::operator()(request_context& context) {
    auto res = process(context);
    if(! res) {
        // Ensure tx aborts on any error. Though tx might be already aborted due to kvs errors,
        // aborting again will do no harm since sharksfin tx manages is_active flag and omits aborting again.
        auto& tx = context.transaction();
        utils::abort_transaction(*tx);
    }
    return res;
}


std::vector<secondary_context> create_secondary_contexts(
    std::vector<secondary_target> const& targets,
    kvs::database& db,
    request_context& context
) {
    std::vector<secondary_context> ret{};
    ret.reserve(targets.size());
    for (auto&& e: targets) {
        ret.emplace_back(
            db.get_or_create_storage(e.storage_name()),
            std::addressof(context));
    }
    return ret;
}

bool write::try_insert_primary(
    write_context& wctx,
    bool& primary_already_exists,
    std::string_view& encoded_primary_key
) {
    primary_already_exists = false;
    if(auto res = primary_.encode_put(
        wctx.primary_context_,
        *wctx.request_context_->transaction(),
        kvs::put_option::create,
        wctx.key_store_.ref(),
        wctx.value_store_.ref(),
        encoded_primary_key
    ); res != status::ok) {
        if (res == status::already_exists) {
            primary_already_exists = true;
            return true;
        }
        handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
        return false;
    }
    wctx.request_context_->enable_stats()->counter(counter_kind::merged).count(1);
    return true;
}

bool write::put_primary(
    write_context& wctx,
    bool& skip_error,
    std::string_view& encoded_primary_key
) {
    skip_error = false;
    kvs::put_option opt = (kind_ == write_kind::insert || kind_ == write_kind::insert_skip) ?
        kvs::put_option::create :
        kvs::put_option::create_or_update;
    if(auto res = primary_.encode_put(
        wctx.primary_context_,
        *wctx.request_context_->transaction(),
        opt,
        wctx.key_store_.ref(),
        wctx.value_store_.ref(),
        encoded_primary_key
    ); res != status::ok) {
        if (opt == kvs::put_option::create && res == status::already_exists) {
            if (kind_ == write_kind::insert) {
                // integrity violation should be handled in SQL layer and forces transaction abort
                // status::already_exists is an internal code, raise it as constraint violation
                set_error(
                    *wctx.request_context_,
                    error_code::unique_constraint_violation_exception,
                    string_builder{} << "Unique constraint violation occurred. Table:" << primary_.storage_name()
                                     << string_builder::to_string,
                    status::err_unique_constraint_violation
                );
                return false;
            }
            // write_kind::insert_skip
            // duplicated key is simply ignored
            // simply set stats 0 in order to mark INSERT IF NOT EXISTS statement executed
            wctx.request_context_->enable_stats()->counter(counter_kind::inserted).count(0);

            // skip error and move to next tuple
            skip_error = true;
            return false;
        }
        handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
        return false;
    }
    auto kind = opt == kvs::put_option::create ? counter_kind::inserted : counter_kind::merged;
    wctx.request_context_->enable_stats()->counter(kind).count(1);
    return true;
}

write_context::write_context(
    request_context& context,
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,    //NOLINT(performance-unnecessary-value-param)
    maybe_shared_ptr<meta::record_meta> value_meta,  //NOLINT(performance-unnecessary-value-param)
    std::vector<secondary_target> const& secondaries,
    kvs::database& db,
    memory::lifo_paged_memory_resource* resource
) :
    request_context_(std::addressof(context)),
    primary_context_(db.get_or_create_storage(storage_name), key_meta, value_meta, std::addressof(context)),
    secondary_contexts_(create_secondary_contexts(secondaries, db, context)),
    key_store_(key_meta, resource),
    value_store_(value_meta, resource)
{}

bool write::put_secondaries(
    write_context& wctx,
    std::string_view encoded_primary_key
) {
    for(std::size_t i=0, n=secondaries_.size(); i < n; ++i) {
        auto& e = secondaries_[i];
        if(auto res = e.encode_put(
            wctx.secondary_contexts_[i],
            *wctx.request_context_->transaction(),
            wctx.key_store_.ref(),
            wctx.value_store_.ref(),
            encoded_primary_key
        ); res != status::ok) {
            handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
            return false;
        }
    }
    return true;
}

bool write::update_secondaries_before_upsert(
    write_context& wctx,
    std::string_view encoded_primary_key,
    bool primary_already_exists
) {
    status res{};
    if(encoded_primary_key.empty()) {
        res = primary_.encode_find(
            wctx.primary_context_,
            *wctx.request_context_->transaction(),
            wctx.key_store_.ref(),
            resource_,
            wctx.primary_context_.extracted_key(),
            wctx.primary_context_.extracted_value(),
            encoded_primary_key
        );
    } else {
        res = primary_.find_by_encoded_key(
            wctx.primary_context_,
            *wctx.request_context_->transaction(),
            encoded_primary_key,
            resource_,
            wctx.primary_context_.extracted_key(),
            wctx.primary_context_.extracted_value()
        );
    }
    if(res != status::ok && res != status::not_found) {
        handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
        return false;
    }

    data::aligned_buffer buf_i{};
    data::aligned_buffer buf_e{};
    //TODO remove found_primary which is always true if dev_try_insert_on_upserting_secondary=true
    bool found_primary = res != status::not_found;
    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        auto& e = secondaries_[i];
        auto& c = wctx.secondary_contexts_[i];
        if (found_primary && primary_already_exists) {
            // try update existing secondary entry
            std::string_view encoded_i{};
            if(auto res = e.create_secondary_key(
                   c,
                   buf_i,
                   wctx.key_store_.ref(),
                   wctx.value_store_.ref(),
                   encoded_primary_key,
                   encoded_i
               );
               res != status::ok) {
                handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
                return false;
            }
            std::string_view encoded_e{};
            if(auto res = e.create_secondary_key(
                   c,
                   buf_e,
                   wctx.primary_context_.extracted_key(),
                   wctx.primary_context_.extracted_value(),
                   encoded_primary_key,
                   encoded_e
               );
               res != status::ok) {
                handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
                return false;
            }
            if(encoded_e.size() != encoded_i.size() ||
               std::memcmp(encoded_i.data(), encoded_e.data(), encoded_e.size()) != 0) {
                // secondary entry needs to be updated - first remove it
                if (auto res = e.remove_by_encoded_key(
                        c,
                        *wctx.request_context_->transaction(),
                        encoded_e); res != status::ok) {
                    handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
                    return false;
                }
            }
        }
        if(auto res = e.encode_put(
            c,
            *wctx.request_context_->transaction(),
            wctx.key_store_.ref(),
            wctx.value_store_.ref(),
            encoded_primary_key
        ); res != status::ok) {
            handle_generic_error(*wctx.request_context_, res, error_code::sql_service_exception);
            return false;
        }
    }
    return true;
}

bool write::process(request_context& context) {  //NOLINT(readability-function-cognitive-complexity)
    auto& tx = context.transaction();
    BOOST_ASSERT(tx);  //NOLINT
    auto* db = tx->database();

    write_context wctx(context,
        idx_->simple_name(),
        key_meta_,
        value_meta_,
        secondaries_,
        *db,
        resource_); // currently common::write uses the same resource for building mirror and executing runtime

    for(auto&& tuple: wrt_->tuples()) {
        utils::checkpoint_holder cph(resource_);
        if(auto res = create_record_from_tuple(
            context,
            tuple,
            key_fields_,
            info_,
            *resource_,
            host_variables_,
            wctx.key_store_
        ); res != status::ok) {
            return false;
        }
        if(auto res = create_record_from_tuple(
            context,
            tuple,
            value_fields_,
            info_,
            *resource_,
            host_variables_,
            wctx.value_store_
        ); res != status::ok) {
            return false;
        }

        if(kind_ == write_kind::insert_overwrite && ! secondaries_.empty()) {
            bool primary_already_exists = true; // default true
            std::string_view encoded_primary_key{};
            if(wctx.request_context_->configuration()->try_insert_on_upserting_secondary()) {
                if(! try_insert_primary(wctx, primary_already_exists, encoded_primary_key)) {
                    return false;
                }
            }
            if(! update_secondaries_before_upsert(wctx, encoded_primary_key, primary_already_exists)) {
                return false;
            }
            if(! primary_already_exists) {
                // there was no entry conflicting with insert, so there is nothing to update
                continue;
            }
        }
        // TODO consider to re-use `encoded_primary_key` above to optimize cost,
        // though value part encoding is still required
        std::string_view encoded_primary_key{};
        bool skip_error = false;
        if(! put_primary(wctx, skip_error, encoded_primary_key)) {
            if(skip_error) {
                continue;
            }
            return false;
        }

        if(kind_ == write_kind::insert_overwrite) {
            // updating secondaries is done already
            continue;
        }

        if(! put_secondaries(wctx, encoded_primary_key)) {
            return false;
        };
    }
    return true;
}

}  // namespace jogasaki::executor::common
