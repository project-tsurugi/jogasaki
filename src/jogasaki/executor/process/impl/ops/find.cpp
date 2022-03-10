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
#include "find.h"

#include <vector>

#include <takatori/util/downcast.h>
#include <takatori/relation/find.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/reader_container.h>
#include <jogasaki/executor/record_writer.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/kvs/coder.h>
#include "operator_base.h"
#include "context_helper.h"
#include "find_context.h"
#include "operator_builder.h"
#include "details/encode_key.h"
#include "details/error_abort.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

find::find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view storage_name,
    std::string_view secondary_storage_name,
    std::vector<details::search_key_field_info> search_key_fields,
    std::vector<details::field_info> key_fields,
    std::vector<details::field_info> value_fields,
    std::vector<details::secondary_index_field_info> secondary_key_fields,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) :
    record_operator(index, info, block_index, input_variable_info, output_variable_info),
    use_secondary_(! secondary_storage_name.empty()),
    storage_name_(storage_name),
    secondary_storage_name_(secondary_storage_name),
    search_key_fields_(std::move(search_key_fields)),
    downstream_(std::move(downstream)),
    field_mapper_(
        use_secondary_,
        std::move(key_fields),
        std::move(value_fields),
        std::move(secondary_key_fields)
    )
{}

find::find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    takatori::tree::tree_fragment_vector<key> const& keys,
    yugawara::storage::index const& primary_idx,
    sequence_view<column const> columns,
    yugawara::storage::index const* secondary_idx,
    std::unique_ptr<operator_base> downstream,
    variable_table_info const* input_variable_info,
    variable_table_info const* output_variable_info
) :
    find(
        index,
        info,
        block_index,
        primary_idx.simple_name(),
        secondary_idx != nullptr ? secondary_idx->simple_name() : "",
        details::create_search_key_fields((secondary_idx != nullptr ? *secondary_idx : primary_idx), keys, info),
        create_fields(primary_idx, columns, (output_variable_info != nullptr ? *output_variable_info : info.vars_info_list()[block_index]), true),
        create_fields(primary_idx, columns, (output_variable_info != nullptr ? *output_variable_info : info.vars_info_list()[block_index]), false),
        create_secondary_key_fields(secondary_idx),
        std::move(downstream),
        input_variable_info,
        output_variable_info
    )
{}

operation_status find::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<class find_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<class find_context>(index(),
            ctx.variable_table(block_index()),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(storage_name()),
            use_secondary_ ? ctx.database()->get_storage(secondary_storage_name()) : nullptr,
            ctx.transaction(),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status find::call_downstream(
    class find_context& ctx,
    std::string_view k,
    std::string_view v,
    accessor::record_ref target,
    context_base::memory_resource* resource,
    abstract::task_context* context
) {
    if (auto res = field_mapper_(k, v, target, *ctx.stg_, *ctx.tx_, resource); res != status::ok) {
        return details::error_abort(ctx, res);
    }
    if (downstream_) {
        if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
            ctx.abort();
            return {operation_status_kind::aborted};
        }
    }
    return {};
}

operation_status find::operator()(class find_context& ctx, abstract::task_context* context) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    auto target = ctx.output_variables().store().ref();
    auto resource = ctx.varlen_resource();
    std::string_view v{};
    executor::process::impl::variable_table vars{};
    std::size_t len{};
    if(auto res = details::encode_key(search_key_fields_, vars, *resource, ctx.key_, len);
        res != status::ok) {
        if (res == status::err_integrity_constraint_violation) {
            // null is assigned for find condition. Nothing should be found.
            finish(context);
            return {};
        }
        return details::error_abort(ctx, res);
    }
    std::string_view k{static_cast<char*>(ctx.key_.data()), len};
    if (! use_secondary_) {
        auto& stg = *ctx.stg_;
        if(auto res = stg.get(*ctx.tx_, k, v); res != status::ok) {
            finish(context);
            if (res == status::not_found) {
                return {};
            }
            return details::error_abort(ctx, res);
        }
        auto ret = call_downstream(ctx, k, v, target, resource, context);
        finish(context);
        return ret;
    }
    auto& stg = *ctx.secondary_stg_;
    std::unique_ptr<kvs::iterator> it{};
    if(auto res = stg.scan(*ctx.tx_,
            k, kvs::end_point_kind::prefixed_inclusive,
            k, kvs::end_point_kind::prefixed_inclusive,
            it
        ); res != status::ok) {
        finish(context);
        if (res == status::not_found) {
            return {};
        }
        return details::error_abort(ctx, res);
    }
    while(true) {
        if(auto res = it->next(); res != status::ok) {
            finish(context);
            if (res == status::not_found) {
                return {};
            }
            return details::error_abort(ctx, res);
        }
        if(auto res = it->key(k); res != status::ok) {
            finish(context);
            // shirakami returns not_found here even if next() above returns ok. TODO confirm contract
            if (res == status::not_found) {
                return {};
            }
            return details::error_abort(ctx, res);
        }
        if(auto ret = call_downstream(ctx, k, v, target, resource, context); ! ret) {
            finish(context);
            return ret;
        }
    }
    finish(context);
    return {};
}

operator_kind find::kind() const noexcept {
    return operator_kind::find;
}

std::string_view find::storage_name() const noexcept {
    return storage_name_;
}

std::string_view find::secondary_storage_name() const noexcept {
    return secondary_storage_name_;
}

void find::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if(auto* p = find_context<class find_context>(index(), ctx.contexts())) {
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

std::vector<details::field_info> find::create_fields(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    variable_table_info const& output_variable_info,
    bool key
) {
    std::vector<details::field_info> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    std::unordered_map<variable, variable> table_to_stream{};
    for(auto&& c : columns) {
        table_to_stream.emplace(c.source(), c.destination());
    }
    if (key) {
        ret.reserve(idx.keys().size());
        for(auto&& k : idx.keys()) {
            auto kc = bindings(k.column());
            auto t = utils::type_for(k.column().type());
            auto spec = k.direction() == relation::sort_direction::ascendant ?
                kvs::spec_key_ascending : kvs::spec_key_descending;
            if (table_to_stream.count(kc) == 0) {
                ret.emplace_back(
                    t,
                    false,
                    0,
                    0,
                    k.column().criteria().nullity().nullable(),
                    spec
                );
                continue;
            }
            auto&& var = table_to_stream.at(kc);
            ret.emplace_back(
                t,
                true,
                output_variable_info.at(var).value_offset(),
                output_variable_info.at(var).nullity_offset(),
                k.column().criteria().nullity().nullable(),
                spec
            );
        }
        return ret;
    }
    ret.reserve(idx.values().size());
    for(auto&& v : idx.values()) {
        auto b = bindings(v);
        auto& c = static_cast<yugawara::storage::column const&>(v);
        auto t = utils::type_for(c.type());
        if (table_to_stream.count(b) == 0) {
            ret.emplace_back(
                t,
                false,
                0,
                0,
                c.criteria().nullity().nullable(),
                kvs::spec_value
            );
            continue;
        }
        auto&& var = table_to_stream.at(b);
        ret.emplace_back(
            t,
            true,
            output_variable_info.at(var).value_offset(),
            output_variable_info.at(var).nullity_offset(),
            c.criteria().nullity().nullable(),
            kvs::spec_value
        );
    }
    return ret;
}

std::vector<details::secondary_index_field_info> find::create_secondary_key_fields(
    yugawara::storage::index const* idx
) {
    if (idx == nullptr) {
        return {};
    }
    std::vector<details::secondary_index_field_info> ret{};
    ret.reserve(idx->keys().size());
    yugawara::binding::factory bindings{};
    for(auto&& k : idx->keys()) {
        auto kc = bindings(k.column());
        auto t = utils::type_for(k.column().type());
        auto spec = k.direction() == relation::sort_direction::ascendant ?
            kvs::spec_key_ascending : kvs::spec_key_descending;
        ret.emplace_back(
            t,
            k.column().criteria().nullity().nullable(),
            spec
        );
    }
    return ret;
}

}