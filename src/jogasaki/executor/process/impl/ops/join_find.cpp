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
#include <jogasaki/kvs/writable_stream.h>
#include "operator_base.h"
#include "context_helper.h"
#include "join_find_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

namespace details {

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

std::vector<details::secondary_index_field_info> create_secondary_key_fields(
    std::vector<details::join_find_key_field> const& key_fields
) {
    std::vector<details::secondary_index_field_info> ret{};
    ret.reserve(key_fields.size());
    for(auto&& f : key_fields) {
        ret.emplace_back(
            f.type_,
            f.nullable_,
            f.spec_
        );
    }
    return ret;
}

matcher::matcher(
    bool use_secondary,
    std::vector<details::join_find_key_field> const& key_fields,
    std::vector<details::field_info> key_columns,
    std::vector<details::field_info> value_columns
) :
    use_secondary_(use_secondary),
    key_fields_(key_fields),
    field_mapper_(
        use_secondary_,
        std::move(key_columns),
        std::move(value_columns),
        create_secondary_key_fields(key_fields_)
    )
{}

bool matcher::operator()(
    variable_table& vars,
    kvs::storage& primary_stg,
    kvs::storage* secondary_stg,
    kvs::transaction& tx,
    matcher::memory_resource* resource
) {
    std::size_t len = 0;
    for(std::size_t loop=0; loop < 2; ++loop) {
        kvs::writable_stream s{buf_.data(), len};
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
    auto ref = vars.store().ref();

    std::unique_ptr<kvs::iterator> it{}; // keep iterator here so that the key data is alive while mapper is operating
    if (! use_secondary_) {
        auto res = primary_stg.get(tx, key, value);
        status_ = res;
        if (res != status::ok) {
            return false;
        }
    } else {
        auto& stg = *secondary_stg;
        if(auto res = stg.scan(tx,
                key, kvs::end_point_kind::prefixed_inclusive,
                key, kvs::end_point_kind::prefixed_inclusive,
                it
            ); res != status::ok) {
            if (res == status::not_found) {
                status_ = res;
                return false;
            }
            fail();
        }
        if(auto res = it->next(); res != status::ok) {
            fail();
        }
        if(auto res = it->key(key); ! res) {
            fail();
        }
    }
    return field_mapper_(key, value, ref, primary_stg, tx, resource) == status::ok;
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
        p = ctx.make_context<class join_find_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(primary_storage_name_),
            use_secondary_ ? ctx.database()->get_storage(secondary_storage_name_) : nullptr,
            ctx.transaction(),
            std::make_unique<details::matcher>(
                use_secondary_,
                key_fields_,
                key_columns_,
                value_columns_
            ),
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

    if((*ctx.matcher_)(ctx.variables(), *ctx.primary_stg_, ctx.secondary_stg_.get(), *ctx.tx_, resource)) {
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
    return primary_storage_name_;
}

void join_find::finish(abstract::task_context* context) {
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

std::vector<details::field_info> join_find::create_columns(
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

std::vector<details::join_find_key_field> join_find::create_key_fields(
    yugawara::storage::index const& primary_or_secondary_idx,
    takatori::tree::tree_fragment_vector<key> const& keys,
    processor_info const& info
) {
    BOOST_ASSERT(primary_or_secondary_idx.keys().size() == keys.size());  //NOLINT
    std::vector<details::join_find_key_field> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};

    std::unordered_map<variable, takatori::scalar::expression const*> var_to_expression{};
    for(auto&& k : keys) {
        var_to_expression.emplace(k.variable(), &k.value());
    }

    ret.reserve(primary_or_secondary_idx.keys().size());
    for(auto&& k : primary_or_secondary_idx.keys()) {
        auto kc = bindings(k.column());
        auto t = utils::type_for(k.column().type());
        auto spec = k.direction() == relation::sort_direction::ascendant ?
            kvs::spec_key_ascending : kvs::spec_key_descending;
        auto* exp = var_to_expression.at(kc);
        ret.emplace_back(
            t,
            k.column().criteria().nullity().nullable(),
            spec,
            expression::evaluator{*exp, info.compiled_info(), info.host_variables()}
        );
    }
    return ret;
}

join_find::join_find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    std::string_view primary_storage_name,
    std::string_view secondary_storage_name,
    std::vector<details::field_info> key_columns,
    std::vector<details::field_info> value_columns,
    std::vector<details::join_find_key_field> key_fields,
    takatori::util::optional_ptr<takatori::scalar::expression const> condition,
    std::unique_ptr<operator_base> downstream
) noexcept:
    record_operator(index, info, block_index),
    use_secondary_(! secondary_storage_name.empty()),
    primary_storage_name_(primary_storage_name),
    secondary_storage_name_(secondary_storage_name),
    key_columns_(std::move(key_columns)),
    value_columns_(std::move(value_columns)),
    key_fields_(std::move(key_fields)),
    condition_(std::move(condition)),
    downstream_(std::move(downstream)),
    evaluator_(condition_ ?
        expression::evaluator{*condition_, info.compiled_info(), info.host_variables()} :
        expression::evaluator{}
    )
{}

join_find::join_find(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    yugawara::storage::index const& primary_idx,
    sequence_view<column const> columns,
    takatori::tree::tree_fragment_vector<key> const& keys,
    takatori::util::optional_ptr<takatori::scalar::expression const> condition,
    yugawara::storage::index const* secondary_idx,
    std::unique_ptr<operator_base> downstream
) :
    join_find(
        index,
        info,
        block_index,
        primary_idx.simple_name(),
        secondary_idx != nullptr ? secondary_idx->simple_name() : "",
        create_columns(
            primary_idx,
            columns,
            info,
            block_index,
            true
        ),
        create_columns(
            primary_idx,
            columns,
            info,
            block_index,
            false
        ),
        create_key_fields(
            secondary_idx != nullptr ? *secondary_idx : primary_idx,
            keys,
            info
        ),
        condition,
        std::move(downstream)
    )
{}

std::vector<details::field_info> const& join_find::key_columns() const noexcept {
    return key_columns_;
}

std::vector<details::field_info> const& join_find::value_columns() const noexcept {
    return value_columns_;
}

std::vector<details::join_find_key_field> const& join_find::key_fields() const noexcept {
    return key_fields_;
}


}


