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

namespace details {

details::find_field::find_field(
    meta::field_type type,
    bool target_exists,
    std::size_t target_offset,
    std::size_t target_nullity_offset,
    bool source_nullable,
    kvs::coding_spec spec
) :
    type_(std::move(type)),
    target_exists_(target_exists),
    target_offset_(target_offset),
    target_nullity_offset_(target_nullity_offset),
    source_nullable_(source_nullable),
    spec_(spec)
{}

}

ops::find::find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view storage_name,
    std::string_view key,
    std::vector<details::find_field> key_fields,
    std::vector<details::find_field> value_fields,
    std::unique_ptr<operator_base> downstream
) :
    record_operator(index, info, block_index),
    storage_name_(storage_name),
    key_(key),
    key_fields_(std::move(key_fields)),
    value_fields_(std::move(value_fields)),
    downstream_(std::move(downstream))
{}

find::find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view storage_name,
    std::string_view key,
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    std::unique_ptr<operator_base> downstream
) :
    find(
        index,
        info,
        block_index,
        storage_name,
        key,
        create_fields(idx, columns, info, block_index, true),
        create_fields(idx, columns, info, block_index, false),
        std::move(downstream)
    )
{}

operation_status find::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<class find_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<class find_context>(index(),
            ctx.block_scope(block_index()),
            ctx.database()->get_storage(storage_name()),
            ctx.transaction(),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status find::operator()(class find_context& ctx, abstract::task_context* context) {
    auto target = ctx.variables().store().ref();
    auto resource = ctx.varlen_resource();
    std::string_view v{};
    if(auto res = ctx.stg_->get(*ctx.tx_, key_, v); res != status::ok) {
        if (res == status::not_found) {
            return {};
        }
        ctx.state(context_state::abort);
        ctx.req_context()->status_code(res);
        return {operation_status_kind::aborted};
    }
    kvs::stream keys{key_.data(), key_.size()}; //TODO create read-only stream
    kvs::stream values{const_cast<char*>(v.data()), v.length()}; //   and avoid using const_cast
    decode_fields(key_fields_, keys, target, resource);
    decode_fields(value_fields_, values, target, resource);
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
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

void find::finish(abstract::task_context*) {
    fail();
}

void find::decode_fields(
    std::vector<details::find_field> const& fields,
    kvs::stream& stream,
    accessor::record_ref target,
    find::memory_resource* resource
) {
    for(auto&& f : fields) {
        if (! f.target_exists_) {
            if (f.source_nullable_) {
                kvs::consume_stream_nullable(stream, f.type_, f.spec_);
                continue;
            }
            kvs::consume_stream(stream, f.type_, f.spec_);
            continue;
        }
        if (f.source_nullable_) {
            kvs::decode_nullable(stream, f.type_, f.spec_, target, f.target_offset_, f.target_nullity_offset_, resource);
            continue;
        }
        kvs::decode(stream, f.type_, f.spec_, target, f.target_offset_, resource);
        target.set_null(f.target_nullity_offset_, false); // currently assuming target variable fields are nullable and f.target_nullity_offset_ is valid even if f.source_nullable_ is false
    }
}

std::vector<details::find_field> find::create_fields(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    processor_info const& info,
    operator_base::block_index_type block_index,
    bool key
) {
    std::vector<details::find_field> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    std::unordered_map<variable, variable> table_to_stream{};
    for(auto&& c : columns) {
        table_to_stream.emplace(c.source(), c.destination());
    }
    auto& block = info.scopes_info()[block_index];
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
                block.value_map().at(var).value_offset(),
                block.value_map().at(var).nullity_offset(),
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
            block.value_map().at(var).value_offset(),
            block.value_map().at(var).nullity_offset(),
            c.criteria().nullity().nullable(),
            kvs::spec_value
        );
    }
    return ret;
}

}


