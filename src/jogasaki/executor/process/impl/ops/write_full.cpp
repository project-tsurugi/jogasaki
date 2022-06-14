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
#include "write_full.h"

#include <vector>

#include <yugawara/binding/factory.h>

#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/error.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/as_any.h>
#include <jogasaki/utils/coder.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include "operator_base.h"
#include "context_helper.h"
#include "details/error_abort.h"

namespace jogasaki::executor::process::impl::ops {

write_full::write_full(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    std::string_view storage_name,
    std::vector<details::write_full_field> key_fields,
    std::vector<details::write_full_field> value_fields,
    variable_table_info const* input_variable_info
) :
    record_operator(index, info, block_index, input_variable_info, nullptr),
    kind_(kind),
    storage_name_(storage_name),
    key_fields_(std::move(key_fields)),
    value_fields_(std::move(value_fields))
{}

variable_table_info const& input_info(
    processor_info const& info,
    variable_table_info const* input_variable_info,
    operator_base::block_index_type block_index
) {
    return input_variable_info != nullptr ? *input_variable_info : info.vars_info_list()[block_index];
}

write_full::write_full(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    yugawara::storage::index const& idx,
    sequence_view<key const> keys,
    sequence_view<column const> columns,
    variable_table_info const* input_variable_info
) :
    write_full(
        index,
        info,
        block_index,
        kind,
        idx.simple_name(),
        create_fields(kind, idx, keys, columns, input_info(info, input_variable_info, block_index), true),
        create_fields(kind, idx, keys, columns, input_info(info, input_variable_info, block_index), false),
        input_variable_info
    )
{}

operation_status write_full::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<write_full_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<write_full_context>(index(),
            ctx.variable_table(block_index()),
            ctx.database()->get_storage(storage_name()),
            ctx.transaction(),
            ctx.req_context()->sequence_manager(),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p);
}

operation_status write_full::operator()(write_full_context& ctx) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    switch(kind_) {
        case write_kind::insert:
            return do_insert(ctx);
        case write_kind::insert_overwrite:
            return do_insert(ctx);
        case write_kind::delete_:
            return do_delete(ctx);
        default:
            fail();
    }
}

operator_kind write_full::kind() const noexcept {
    return operator_kind::write_full;
}

std::string_view write_full::storage_name() const noexcept {
    return storage_name_;
}

void write_full::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if(auto* p = find_context<write_full_context>(index(), ctx.contexts())) {
        p->release();
    }
}

void write_full::encode_fields(
    std::vector<details::write_full_field> const& fields,
    kvs::writable_stream& stream,
    accessor::record_ref source
) {
    for(auto const& f : fields) {
        if(f.nullable_) {
            kvs::encode_nullable(source, f.offset_, f.nullity_offset_, f.type_, f.spec_, stream);
        } else {
            kvs::encode(source, f.offset_, f.type_, f.spec_, stream);
        }
    }
}

void create_generated_field(
    std::vector<details::write_full_field>& ret,
    yugawara::storage::column_value const& dv,
    takatori::type::data const& type,
    bool nullable,
    kvs::coding_spec spec
) {
    using yugawara::storage::column_value_kind;
    auto knd = default_value_kind::nothing;
    sequence_definition_id def_id{};
    data::aligned_buffer buf{};
    auto t = utils::type_for(type);
    switch(dv.kind()) {
        case column_value_kind::nothing:
            break;
        case column_value_kind::immediate: {
            knd = default_value_kind::immediate;
            auto src = utils::as_any(
                *dv.element<column_value_kind::immediate>(),
                type,
                nullptr
            );
            utils::encode_any( buf, t, nullable, spec, {src});
            break;
        }
        case column_value_kind::sequence: {
            knd = default_value_kind::sequence;
            if (auto id = dv.element<column_value_kind::sequence>()->definition_id()) {
                def_id = *id;
            } else {
                fail("sequence must be defined with definition_id");
            }
        }
    }
    ret.emplace_back(
        t,
        0,
        0,
        nullable,
        spec,
        knd,
        static_cast<std::string_view>(buf),
        def_id
    );
}

std::vector<details::write_full_field> write_full::create_fields(
    write_kind kind,
    yugawara::storage::index const& idx,
    sequence_view<key const> keys,
    sequence_view<column const> columns,
    variable_table_info const& input_variable_info,
    bool key
) {
    std::vector<details::write_full_field> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    std::unordered_map<variable, variable> table_to_stream{};
    auto& map_source = kind == write_kind::insert ? columns :
        (kind == write_kind::delete_ ? keys :
            (key ? keys : columns)
        );
    for(auto&& c : map_source) {
        table_to_stream.emplace(c.destination(), c.source());
    }
    if (key) {
        ret.reserve(idx.keys().size());
        for(auto&& k : idx.keys()) {
            auto kc = bindings(k.column());
            auto& type = k.column().type();
            auto t = utils::type_for(type);
            auto spec = k.direction() == relation::sort_direction::ascendant ?
                kvs::spec_key_ascending : kvs::spec_key_descending;
            if (table_to_stream.count(kc) == 0) {
                if (kind == write_kind::delete_) {
                    fail();
                }
                // no column specified - use default value
                auto& dv = k.column().default_value();
                bool nullable = k.column().criteria().nullity().nullable();
                create_generated_field(ret, dv, type, nullable, spec);
                continue;
            }
            auto&& var = table_to_stream.at(kc);
            ret.emplace_back(
                t,
                input_variable_info.at(var).value_offset(),
                input_variable_info.at(var).nullity_offset(),
                k.column().criteria().nullity().nullable(),
                spec
            );
        }
        return ret;
    }
    if (kind == write_kind::delete_) {
        // delete requires only key fields
        return ret;
    }
    ret.reserve(idx.values().size());
    for(auto&& v : idx.values()) {
        auto b = bindings(v);
        auto& c = static_cast<yugawara::storage::column const&>(v);
        auto& type = c.type();
        auto t = utils::type_for(type);
        if (table_to_stream.count(b) == 0) {
            // no column specified - use default value
            auto& dv = c.default_value();
            bool nullable = c.criteria().nullity().nullable();
            create_generated_field(ret, dv, type, nullable, kvs::spec_value);
            continue;
        }
        auto&& var = table_to_stream.at(b);
        ret.emplace_back(
            t,
            input_variable_info.at(var).value_offset(),
            input_variable_info.at(var).nullity_offset(),
            c.criteria().nullity().nullable(),
            kvs::spec_value
        );
    }
    return ret;
}

operation_status write_full::do_insert(write_full_context& ctx) {
    auto source = ctx.input_variables().store().ref();
    // calculate length first, then put
    check_length_and_extend_buffer(ctx, key_fields_, ctx.key_buf_, source);
    check_length_and_extend_buffer(ctx, value_fields_, ctx.value_buf_, source);
    kvs::writable_stream keys{ctx.key_buf_.data(), ctx.key_buf_.size()};
    kvs::writable_stream values{ctx.value_buf_.data(), ctx.value_buf_.size()};
    encode_fields(key_fields_, keys, source);
    encode_fields(value_fields_, values, source);
    kvs::put_option opt = kind_ == write_kind::insert ?
        kvs::put_option::create :
        kvs::put_option::create_or_update;
    if(auto res = ctx.stg_->put(
            *ctx.tx_,
            {keys.data(), keys.size()},
            {values.data(), values.size()},
            opt
        ); ! is_ok(res)) {
        return details::error_abort(ctx, res);
    }
    return {};
}

void write_full::check_length_and_extend_buffer(
    write_full_context&,
    std::vector<details::write_full_field> const& fields,
    data::aligned_buffer& buffer,
    accessor::record_ref source
) {
    kvs::writable_stream null_stream{};
    encode_fields(fields, null_stream, source);
    if (null_stream.size() > buffer.size()) {
        buffer.resize(null_stream.size());
    }
}

operation_status write_full::do_delete(write_full_context& ctx) {
    auto k = prepare_key(ctx);
    if(auto res = ctx.stg_->remove(*ctx.tx_, k ); is_error(res)) {
        if(res == status::err_aborted_retryable) {
            return details::error_abort(ctx, res);
        }
        fail();
    }
    // warning such as status::not_found are safely ignored for delete
    return {};
}

std::string_view write_full::prepare_key(write_full_context& ctx) {
    auto source = ctx.input_variables().store().ref();
    // calculate length first, and then put
    check_length_and_extend_buffer(ctx, key_fields_, ctx.key_buf_, source);
    kvs::writable_stream keys{ctx.key_buf_.data(), ctx.key_buf_.size()};
    encode_fields(key_fields_, keys, source);
    return {keys.data(), keys.size()};
}

}


