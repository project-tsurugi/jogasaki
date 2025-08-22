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
#include "write_existing.h"

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
#include <takatori/util/exception.h>
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
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/expr/details/cast_evaluation.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/write_existing_context.h>
#include <jogasaki/executor/process/impl/ops/write_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/index/primary_context.h>
#include <jogasaki/index/primary_target.h>
#include <jogasaki/index/secondary_context.h>
#include <jogasaki/index/secondary_target.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/plan_exception.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_statistics.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/fail.h>
#include <jogasaki/utils/field_types.h>

#include "context_helper.h"
#include "details/error_abort.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using variable = takatori::descriptor::variable;
using takatori::util::string_builder;
using takatori::util::throw_exception;

void write_existing::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if(auto* p = find_context<write_existing_context>(index(), ctx.contexts())) {
        p->release();
    }
}

std::string_view write_existing::storage_name() const noexcept {
    return primary_.storage_name();
}

operator_kind write_existing::kind() const noexcept {
    return operator_kind::write_existing;
}

operation_status write_existing::operator()(write_existing_context& ctx) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    switch(kind_) {
        case write_kind::update:
            return do_update(ctx);
        case write_kind::delete_:
            return do_delete(ctx);
        default:
            fail_with_exception();
    }
}

static void abort_transaction(transaction_context& tx) {
    if (auto res = tx.abort_transaction(); res != status::ok) {
        throw_exception(std::logic_error{"abort failed unexpectedly"});
    }
}

status update_record(
    std::vector<details::update_field>& fields,
    request_context & ctx,
    memory::lifo_paged_memory_resource* resource,
    accessor::record_ref extracted_key_record,
    accessor::record_ref extracted_value_record,
    accessor::record_ref input_variables,
    accessor::record_ref host_variables
) {
    for(auto const& f : fields) {
        auto target = f.key_ ? extracted_key_record : extracted_value_record;
        if(! f.requires_conversion_) {
            // assuming intermediate fields are nullable. Nullability check is done on encoding.
            utils::copy_nullable_field(
                f.target_ftype_,
                target,
                f.target_offset_,
                f.target_nullity_offset_,
                f.source_external_ ? host_variables : input_variables,
                f.source_offset_,
                f.source_nullity_offset_
            );
            continue;
        }
        data::any a{};
        utils::copy_nullable_field_as_any(
            f.source_ftype_,
            f.source_external_ ? host_variables : input_variables,
            f.source_offset_,
            f.source_nullity_offset_,
            a,
            nullptr // varlen data is owned by the source record
        );

        // To clean up varlen data resource in data::any, we rely on upper layer that does clean up
        // on evey process invocation. Otherwise, we have to copy the result of conversion and
        // lifo resource is not convenient to copy the result when caller and callee use the same resource.
        data::any converted{};
        if(auto res = conv::conduct_assignment_conversion(
               *f.source_type_,
               *f.target_type_,
               a,
               converted,
               ctx,
               resource
           );
           res != status::ok) {
            return res;
        }
        utils::copy_nullable_field(
            f.target_ftype_,
            target,
            f.target_offset_,
            f.target_nullity_offset_,
            converted
        );
    }
    return status::ok;
}

static bool updates_key(std::vector<details::update_field> const& updates) noexcept {
    return std::any_of(updates.begin(), updates.end(), [](auto const& f) {
        return f.key_;
    });
}

operation_status write_existing::do_update(write_existing_context& ctx) {
    auto& context = ctx.primary_context();
    // find update target and fill internal extracted key/values in primary target
    std::string_view encoded{};
    if(auto res = primary_.encode_find(
            context,
            *ctx.transaction()->object(),
            ctx.input_variables().store().ref(),
            ctx.varlen_resource(),
            context.extracted_key(),
            context.extracted_value(),
            encoded
        ); res != status::ok) {
        abort_transaction(*ctx.transaction());
        return error_abort(ctx, res);
    }

    if(primary_key_updated_) {
        // remove and recreate records
        if(auto res = primary_.remove_by_encoded_key(
                context,
                *ctx.transaction(),
                encoded
            ); res != status::ok) {
            abort_transaction(*ctx.transaction());
            return error_abort(ctx, res);
        }
    }

    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        if(! primary_key_updated_ && ! secondary_key_updated_[i]) {
            continue;
        }
        if(auto res = secondaries_[i].encode_remove(
            ctx.secondary_contexts_[i],
            *ctx.transaction(),
            context.extracted_key(),
            context.extracted_value(),
            context.encoded_key()
        ); res != status::ok) {
            abort_transaction(*ctx.transaction());
            return error_abort(ctx, res);
        }
    }

    // update extracted key/value in primary target with values from variable table
    if(auto res = update_record(
           updates_,
           *ctx.req_context(),
           ctx.resource(),
           context.extracted_key(),
           context.extracted_value(),
           ctx.input_variables().store().ref(),
           host_variables() ? host_variables()->store().ref() : accessor::record_ref{}
       ); res != status::ok) {
            abort_transaction(*ctx.transaction());
            return error_abort(ctx, res);
    }

    // encode extracted key/value in primary target and send to kvs
    kvs::put_option opt = primary_key_updated_ ? kvs::put_option::create : kvs::put_option::update;
    std::string_view encoded_key{};
    if(auto res = primary_.encode_put(
           context,
           *ctx.transaction(),
           opt,
           context.extracted_key(),
           context.extracted_value(),
           encoded_key
       );
       res != status::ok) {
        abort_transaction(*ctx.transaction());
        if(res == status::already_exists) {
            res = status::err_unique_constraint_violation;
        }
        return error_abort(ctx, res);
    }
    if(context.req_context()) {
        context.req_context()->enable_stats()->counter(counter_kind::updated).count(1);
    }

    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        if(! primary_key_updated_ && ! secondary_key_updated_[i]) {
            continue;
        }
        if(auto res = secondaries_[i].encode_put(
                ctx.secondary_contexts_[i],
                *ctx.transaction(),
                context.extracted_key(),
                context.extracted_value(),
                context.encoded_key()
            ); res != status::ok) {
            abort_transaction(*ctx.transaction());
            return error_abort(ctx, res);
        }
    }
    return {};
}

operation_status write_existing::do_delete(write_existing_context& ctx) {
    auto& context = ctx.primary_context();
    if(secondaries_.empty()) {
        if(auto res = primary_.encode_remove(
                context,
                *ctx.transaction(),
                ctx.input_variables().store().ref()
            ); res != status::ok) {
            return error_abort(ctx, res);
        }
        if(context.req_context()) {
            context.req_context()->enable_stats()->counter(counter_kind::deleted).count(1);
        }
        return {};
    }

    if(auto res = primary_.encode_find_remove(
            context,
            *ctx.transaction(),
            ctx.input_variables().store().ref(),
            ctx.varlen_resource(),
            context.extracted_key(),
            context.extracted_value()
        ); res != status::ok) {
        return error_abort(ctx, res);
    }
    if(context.req_context()) {
        context.req_context()->enable_stats()->counter(counter_kind::deleted).count(1);
    }

    for(std::size_t i=0, n=secondaries_.size(); i<n; ++i) {
        if(auto res = secondaries_[i].encode_remove(
                ctx.secondary_contexts_[i],
                *ctx.transaction(),
                context.extracted_key(),
                context.extracted_value(),
                context.encoded_key()
            ); res != status::ok) {
            return error_abort(ctx, res);
        }
    }
    return {};
}

operation_status write_existing::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<write_existing_context>(index(), ctx.contexts());
    if (! p) {
        std::vector<index::secondary_context> contexts{};
        contexts.reserve(secondaries_.size());
        for(auto&& s : secondaries_) {
            contexts.emplace_back(
                ctx.database()->get_or_create_storage(s.storage_name()),
                ctx.req_context()
            );
        }
        p = ctx.make_context<write_existing_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(storage_name()),
            ctx.transaction(),
            primary_.key_meta(),
            primary_.value_meta(),
            ctx.resource(),
            ctx.varlen_resource(),
            std::move(contexts)
        );
    }
    return (*this)(*p);
}

static std::tuple<std::size_t, std::size_t, bool> resolve_variable_offsets(
    variable_table_info const& block_variables,
    variable_table_info const* host_variables,
    variable_table_info::variable const& src
) {
    if (block_variables.exists(src)) {
        return {
            block_variables.at(src).value_offset(),
            block_variables.at(src).nullity_offset(),
            false
        };
    }
    BOOST_ASSERT(host_variables != nullptr && host_variables->exists(src));  //NOLINT
    return {
        host_variables->at(src).value_offset(),
        host_variables->at(src).nullity_offset(),
        true
    };
}

std::vector<details::update_field> create_update_fields(
    yugawara::storage::index const& idx,
    sequence_view<takatori::relation::write::key const> keys, // keys to identify the updated record, possibly part of idx.keys()
    sequence_view<takatori::relation::write::column const> columns, // columns to be updated
    variable_table_info const* host_variable_info,
    variable_table_info const& input_variable_info,
    yugawara::compiled_info const& cinfo
) {
    std::vector<details::update_field> ret{};
    yugawara::binding::factory bindings{};
    std::unordered_map<variable, variable> key_dest_to_src{};
    std::unordered_map<variable, variable> column_dest_to_src{};
    for(auto&& c : keys) {
        key_dest_to_src.emplace(c.destination(), c.source());
    }
    for(auto&& c : columns) {
        column_dest_to_src.emplace(c.destination(), c.source());
    }

    ret.reserve(idx.keys().size()+idx.values().size());
    {
        auto meta = index::create_meta(idx, true);
        for(std::size_t i=0, n=idx.keys().size(); i<n; ++i) {
            auto&& k = idx.keys()[i];
            auto kc = bindings(k.column());
            if (key_dest_to_src.count(kc) == 0) {
                // throw_exception(std::logic_error{""}); // TODO update by non-unique keys
            }
            if (column_dest_to_src.count(kc) != 0) {
                if(k.column().features().contains(::yugawara::storage::column_feature::read_only)) {
                    auto msg =
                        string_builder{}<< "write operation on read-only column name:" << k.column().simple_name()
                        << string_builder::to_string;
                    throw_exception(plan::plan_exception{
                        create_error_info(error_code::restricted_operation_exception, msg, status::err_illegal_operation)
                    });
                }
                auto&& src = column_dest_to_src.at(kc);
                auto& src_type = cinfo.type_of(src);
                auto [os, nos, b] = resolve_variable_offsets(input_variable_info, host_variable_info, src);
                ret.emplace_back(
                    src_type,
                    k.column().type(),
                    os,
                    nos,
                    meta->value_offset(i),
                    meta->nullity_offset(i),
                    k.column().criteria().nullity().nullable(),
                    b,
                    true
                );
            }
        }
    }
    auto meta = index::create_meta(idx, false);
    for(std::size_t i=0, n=idx.values().size(); i<n; ++i) {
        auto&& v = idx.values()[i];
        auto b = bindings(v);
        auto& c = static_cast<yugawara::storage::column const&>(v);
        if (column_dest_to_src.count(b) != 0) {
            if(c.features().contains(::yugawara::storage::column_feature::read_only)) {
                auto msg = string_builder{} << "write operation on read-only column name:" << c.simple_name()
                                            << string_builder::to_string;
                throw_exception(plan::plan_exception{
                    create_error_info(error_code::restricted_operation_exception, msg, status::err_illegal_operation)
                });
            }
            auto&& src = column_dest_to_src.at(b);
            auto& src_type = cinfo.type_of(src);
            auto [os, nos, src_is_external ] = resolve_variable_offsets(input_variable_info, host_variable_info, src);
            ret.emplace_back(
                src_type,
                c.type(),
                os,
                nos,
                meta->value_offset(i),
                meta->nullity_offset(i),
                c.criteria().nullity().nullable(),
                src_is_external,
                false
            );
        }
    }
    return ret;
}

static bool overwraps(
    std::vector<yugawara::storage::index::key> const& keys,
    sequence_view<write_existing::column const> columns
) {
    yugawara::binding::factory bindings{};
    for(auto&& k : keys) {
        auto kc = bindings(k.column());
        for(auto&& c : columns) {
            if(c.destination() == kc) {
                return true;
            }
        }
    }
    return false;
}

static std::pair<std::vector<index::secondary_target>, write_existing::bool_list_type>
create_secondary_targets_and_key_update_list(
    yugawara::storage::index const& idx,
    sequence_view<write_existing::column const> columns
) {
    auto& table = idx.table();
    auto& primary = *table.owner()->find_primary_index(table);
    auto key_meta = index::create_meta(primary, true);
    auto value_meta = index::create_meta(primary, false);
    std::vector<index::secondary_target> ret_l{};
    write_existing::bool_list_type ret_r{};
    std::size_t count{};
    table.owner()->each_table_index(table,
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (*entry == idx) {
                return;
            }
            ++count;
        }
    );
    ret_l.reserve(count);
    ret_r.resize(count);
    std::size_t i = 0;
    table.owner()->each_table_index(table,
        [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const& entry) {
            if (*entry == idx) {
                return;
            }
            ret_l.emplace_back(
                *entry,
                key_meta,
                value_meta
            );
            ret_r[i] = overwraps(entry->keys(), columns);
            ++i;
        }
    );
    return {ret_l, ret_r};
}

static std::vector<index::secondary_target> create_secondary_targets(
    yugawara::storage::index const& idx,
    sequence_view<write_existing::column const> columns
) {
    auto [tgts, updates] = create_secondary_targets_and_key_update_list(idx, columns);
    (void) updates;
    return tgts;
}

static write_existing::bool_list_type create_secondary_key_updated(
    yugawara::storage::index const& idx,
    sequence_view<write_existing::column const> columns
) {
    auto [tgts, updates] = create_secondary_targets_and_key_update_list(idx, columns);
    (void) tgts;
    return updates;
}

write_existing::write_existing(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    yugawara::storage::index const& idx,
    sequence_view<key const> keys,
    sequence_view<column const> columns,
    variable_table_info const* input_variable_info
) :
    write_existing(
        index,
        info,
        block_index,
        kind,
        index::primary_target{
            idx,
            keys,
            input_variable_info ? *input_variable_info : info.vars_info_list()[block_index]
        },
        create_update_fields(
            idx,
            keys,
            columns,
            info.host_variables() ? std::addressof(info.host_variables()->info()) : nullptr,
            input_variable_info ? *input_variable_info : info.vars_info_list()[block_index],
            info.compiled_info()
        ),
        create_secondary_targets(idx, columns),
        create_secondary_key_updated(idx, columns),
        input_variable_info
    )
{}

write_existing::write_existing(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    index::primary_target primary,
    std::vector<details::update_field> updates,
    std::vector<index::secondary_target> secondaries,
    bool_list_type secondary_key_updated,
    variable_table_info const* input_variable_info
) :
    record_operator(index, info, block_index, input_variable_info),
    kind_(kind),
    primary_(std::move(primary)),
    secondaries_(std::move(secondaries)),
    primary_key_updated_(updates_key(updates)),
    secondary_key_updated_(std::move(secondary_key_updated)),
    updates_(std::move(updates))
{}

index::primary_target const& write_existing::primary() const noexcept {
    return primary_;
}

write_kind write_existing::get_write_kind() const noexcept {
    return kind_;
}
}  // namespace jogasaki::executor::process::impl::ops
