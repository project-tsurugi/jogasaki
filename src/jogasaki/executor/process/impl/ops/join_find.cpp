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
#include "join_find.h"

#include <vector>

#include <takatori/util/downcast.h>
#include <takatori/relation/join_find.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/executor/process/step.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/kvs/coder.h>
#include "operator_base.h"
#include "context_helper.h"
#include "join_find_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

namespace details {

join_find_column::join_find_column(
    meta::field_type type,
    bool target_exists,
    std::size_t offset,
    std::size_t nullity_offset,
    bool nullable,
    kvs::coding_spec spec
) :
    type_(std::move(type)),
    target_exists_(target_exists),
    offset_(offset),
    nullity_offset_(nullity_offset),
    nullable_(nullable),
    spec_(spec)
{}

join_find_key_field::join_find_key_field(
    meta::field_type type,
    bool nullable,
    kvs::coding_spec spec,
    expression::evaluator evaluator
) :
    type_(std::move(type)),
    nullable_(nullable),
    spec_(spec),
    evaluator_(evaluator)
{}


matcher::matcher(
    std::vector<details::join_find_key_field> const& key_fields,
    std::vector<details::join_find_column> const& key_columns,
    std::vector<details::join_find_column> const& value_columns
) :
    key_fields_(key_fields),
    key_columns_(key_columns),
    value_columns_(value_columns)
{}

void matcher::read_stream(
    variable_table& vars,
    matcher::memory_resource* resource,
    kvs::stream& src,
    std::vector<details::join_find_column> const& columns
) {
    auto ref = vars.store().ref();
    for(auto& c : columns) {
        if (c.target_exists_) {
            if (c.nullable_) {
                kvs::decode_nullable(src, c.type_, c.spec_, ref, c.offset_, c.nullity_offset_, resource);
                continue;
            }
            kvs::decode(src, c.type_, c.spec_, ref, c.offset_, resource);
            ref.set_null(c.nullity_offset_, false); // currently assuming target variable fields are nullable
                                                          // and f.target_nullity_offset_ is valid even
                                                          // if f.source_nullable_ is false
            continue;
        }
        if (c.nullable_) {
            kvs::consume_stream_nullable(src, c.type_, c.spec_);
            continue;
        }
        kvs::consume_stream(src, c.type_, c.spec_);
    }
}

bool matcher::operator()(
    variable_table& vars,
    kvs::storage& stg,
    kvs::transaction& tx,
    matcher::memory_resource* resource
) {
    std::size_t len = 0;
    for(std::size_t loop=0; loop < 2; ++loop) {
        kvs::stream s{buf_.data(), len};
        auto cp = resource->get_checkpoint();
        for(auto&f : key_fields_) {
            auto any = f.evaluator_(vars, resource);
            if (f.nullable_) {
                kvs::encode_nullable(any, f.type_, f.spec_, s);
            } else {
                BOOST_ASSERT(any.has_value());  //NOLINT
                kvs::encode(any, f.type_, f.spec_, s);
            }
        }
        resource->deallocate_after(cp);
        if (loop == 0) {
            len = s.length();
            if (buf_.size() < len) {
                buf_.resize(len);
            }
        }
    }
    std::string_view key{static_cast<char*>(buf_.data()), len};
    std::string_view value{};
    auto res = stg.get(tx, key, value);
    status_ = res;
    if (res != status::ok) {
        return false;
    }
    kvs::stream keys{const_cast<char*>(key.data()), key.size()};
    kvs::stream values{const_cast<char*>(value.data()), value.size()};
    read_stream(vars, resource, keys, key_columns_);
    read_stream(vars, resource, values, value_columns_);
    return true;
}

bool matcher::next() {
    // this matcher supports at most one record
    status_ = status::not_found;
    return false;
}

status matcher::result() const noexcept {
    return status_;
}

}

operation_status join_find::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<class join_find_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<class join_find_context>(index(),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(storage_name()),
            ctx.transaction(),
            std::make_unique<details::matcher>(key_fields_, key_columns_, value_columns_),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status join_find::operator()(class join_find_context& ctx, abstract::task_context* context) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    auto resource = ctx.varlen_resource();
    if((*ctx.matcher_)(ctx.variables(), *ctx.stg_, *ctx.tx_, resource)) {
        if (condition_) {
            auto r = evaluator_(ctx.variables());
            if(r.has_value() && !r.to<bool>()) {
                return {};
            }
        }
        if (downstream_) {
            if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
                ctx.abort();
                return {operation_status_kind::aborted};
            }
            unsafe_downcast<record_operator>(downstream_.get())->finish(context);
        }
        return {};
    }
    if(ctx.matcher_->result() != status::not_found) {
        ctx.state(context_state::abort);
        ctx.req_context()->status_code(ctx.matcher_->result());
        return {operation_status_kind::aborted};
    }
    return {};
}

operator_kind join_find::kind() const noexcept {
    return operator_kind::join_find;
}

std::string_view join_find::storage_name() const noexcept {
    return storage_name_;
}

void join_find::finish(abstract::task_context* context) {
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

std::vector<details::join_find_column> join_find::create_columns(
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    processor_info const& info,
    operator_base::block_index_type block_index,
    bool key
) {
    std::vector<details::join_find_column> ret{};
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

std::vector<details::join_find_key_field> join_find::create_key_fields(
    yugawara::storage::index const& idx,
    takatori::tree::tree_fragment_vector<key> const& keys,
    processor_info const& info
) {
    BOOST_ASSERT(idx.keys().size() == keys.size());  //NOLINT
    std::vector<details::join_find_key_field> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};

    std::unordered_map<variable, takatori::scalar::expression const*> var_to_expression{};
    for(auto&& k : keys) {
        var_to_expression.emplace(k.variable(), &k.value());
    }

    ret.reserve(idx.keys().size());
    for(auto&& k : idx.keys()) {
        auto kc = bindings(k.column());
        auto t = utils::type_for(k.column().type());
        auto spec = k.direction() == relation::sort_direction::ascendant ?
            kvs::spec_key_ascending : kvs::spec_key_descending;
        auto* exp = var_to_expression.at(kc);
        ret.emplace_back(
            t,
            k.column().criteria().nullity().nullable(),
            spec,
            expression::evaluator{*exp, info.compiled_info()}
        );
    }
    return ret;
}

join_find::join_find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view storage_name,
    yugawara::storage::index const& idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<key> const& keys,
    takatori::util::optional_ptr<takatori::scalar::expression const> condition,
    std::unique_ptr<operator_base> downstream
) :
    join_find(
        index,
        info,
        block_index,
        storage_name,
        create_columns(
            idx,
            columns,
            info,
            block_index,
            true
        ),
        create_columns(
            idx,
            columns,
            info,
            block_index,
            false
        ),
        create_key_fields(
            idx,
            keys,
            info
        ),
        condition,
        std::move(downstream)
    )
{}

std::vector<details::join_find_column> const& join_find::key_columns() const noexcept {
    return key_columns_;
}

std::vector<details::join_find_column> const& join_find::value_columns() const noexcept {
    return value_columns_;
}

std::vector<details::join_find_key_field> const& join_find::key_fields() const noexcept {
    return key_fields_;
}

}


