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

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

find::find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view storage_name,
    std::string_view secondary_storage_name,
    std::string_view key,
    std::vector<details::field_info> key_fields,
    std::vector<details::field_info> value_fields,
    std::vector<details::secondary_index_field_info> secondary_key_fields,
    std::unique_ptr<operator_base> downstream
) :
    record_operator(index, info, block_index),
    use_secondary_(! secondary_storage_name.empty()),
    storage_name_(storage_name),
    secondary_storage_name_(secondary_storage_name),
    key_(key),
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
    std::string_view key,
    yugawara::storage::index const& primary_idx,
    sequence_view<column const> columns,
    yugawara::storage::index const* secondary_idx,
    std::unique_ptr<operator_base> downstream
) :
    find(
        index,
        info,
        block_index,
        primary_idx.simple_name(),
        secondary_idx != nullptr ? secondary_idx->simple_name() : "",
        key,
        create_fields(primary_idx, columns, info, block_index, true),
        create_fields(primary_idx, columns, info, block_index, false),
        create_secondary_key_fields(secondary_idx),
        std::move(downstream)
    )
{}

operation_status find::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<class find_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<class find_context>(index(),
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

operation_status find::operator()(class find_context& ctx, abstract::task_context* context) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    auto target = ctx.variables().store().ref();
    auto resource = ctx.varlen_resource();
    std::string_view v{};
    std::string_view k{key_};
    std::unique_ptr<kvs::iterator> it{};
    if (! use_secondary_) {
        auto& stg = *ctx.stg_;
        if(auto res = stg.get(*ctx.tx_, key_, v); res != status::ok) {
            if (res == status::not_found) {
                return {};
            }
            ctx.state(context_state::abort);
            ctx.req_context()->status_code(res);
            return {operation_status_kind::aborted};
        }
    } else {
        auto& stg = *ctx.secondary_stg_;
        if(auto res = stg.scan(*ctx.tx_,
                key_, kvs::end_point_kind::prefixed_inclusive,
                key_, kvs::end_point_kind::prefixed_inclusive,
                it
            ); res != status::ok) {
            if (res == status::not_found) {
                return {};
            }
            ctx.state(context_state::abort);
            ctx.req_context()->status_code(res);
            return {operation_status_kind::aborted};
        }
        if(auto res = it->next(); res != status::ok) {
            fail();
        }
        if(auto res = it->key(k); ! res) {
            fail();
        }
    }
    field_mapper_(k, v, target, *ctx.stg_, *ctx.tx_, resource);
    if (downstream_) {
        if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
            ctx.abort();
            return {operation_status_kind::aborted};
        }
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
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

void find::finish(abstract::task_context*) {
    // top operators decide finish timing on their own
    fail();
}

std::vector<details::field_info> find::create_fields(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    processor_info const& info,
    operator_base::block_index_type block_index,
    bool key
) {
    std::vector<details::field_info> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    std::unordered_map<variable, variable> table_to_stream{};
    for(auto&& c : columns) {
        table_to_stream.emplace(c.source(), c.destination());
    }
    auto& block = info.vars_info_list()[block_index];
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
                block.at(var).value_offset(),
                block.at(var).nullity_offset(),
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
            block.at(var).value_offset(),
            block.at(var).nullity_offset(),
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