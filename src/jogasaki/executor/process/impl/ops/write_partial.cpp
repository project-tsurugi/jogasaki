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
#include "write_partial.h"

#include <vector>

#include <takatori/relation/write.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/error.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/utils/field_types.h>
#include "operator_base.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

details::write_partial_field::write_partial_field(
    meta::field_type type,
    std::size_t variable_offset,
    std::size_t variable_nullity_offset,
    std::size_t target_offset,
    std::size_t target_nullity_offset,
    bool nullable,
    kvs::coding_spec spec,
    bool updated,
    std::size_t update_variable_offset,
    std::size_t update_variable_nullity_offset
) :
    type_(std::move(type)),
    variable_offset_(variable_offset),
    variable_nullity_offset_(variable_nullity_offset),
    target_offset_(target_offset),
    target_nullity_offset_(target_nullity_offset),
    nullable_(nullable),
    spec_(spec),
    updated_(updated),
    update_variable_offset_(update_variable_offset),
    update_variable_nullity_offset_(update_variable_nullity_offset)
{}

std::string_view write_partial::prepare_encoded_key(write_partial_context& ctx) {
    auto source = ctx.variables().store().ref();
    // calculate length first, and then put
    check_length_and_extend_buffer(true, ctx, key_fields_, ctx.key_buf_, source);
    kvs::stream keys{ctx.key_buf_.data(), ctx.key_buf_.size()};
    encode_fields(true, key_fields_, keys, source);
    return {keys.data(), keys.length()};
}

operation_status write_partial::encode_and_put(write_partial_context& ctx) {
    auto key_source = ctx.key_store_.ref();
    auto val_source = ctx.value_store_.ref();
    // calculate length first, then put
    check_length_and_extend_buffer(false, ctx, key_fields_, ctx.key_buf_, key_source);
    check_length_and_extend_buffer(false, ctx, value_fields_, ctx.value_buf_, val_source);
    kvs::stream keys{ctx.key_buf_.data(), ctx.key_buf_.size()};
    kvs::stream values{ctx.value_buf_.data(), ctx.value_buf_.size()};
    encode_fields(false, key_fields_, keys, key_source);
    encode_fields(false, value_fields_, values, val_source);
    if(auto res = ctx.stg_->put(
            *ctx.tx_,
            {keys.data(), keys.length()},
            {values.data(), values.length()}
        ); is_error(res)) {
        if(res == status::err_aborted_retryable) {
            ctx.state(context_state::abort);
            ctx.req_context()->status_code(res);
            return {operation_status_kind::aborted};
        }
        // updating already found record, so err_not_found should never happen
        fail();
    }
    return {};
}

void write_partial::update_record(write_partial_context& ctx) {
    auto variables = ctx.variables().store().ref();
    update_fields(key_fields_, ctx.key_store_.ref(), variables);
    update_fields(value_fields_, ctx.value_store_.ref(), variables);
}

operation_status write_partial::find_record_and_extract(write_partial_context& ctx) {
    auto varlen_resource = ctx.varlen_resource();
    auto k = prepare_encoded_key(ctx);
    std::string_view v{};
    if(auto res = ctx.stg_->get( *ctx.tx_, k, v ); ! is_ok(res)) {
        if(res == status::err_aborted_retryable) {
            ctx.state(context_state::abort);
            ctx.req_context()->status_code(res);
            return {operation_status_kind::aborted};
        }
        // The update target has been identified on the upstream operator such as find,
        // so this lookup must be successful. If the control reaches here, it's internal error.
        fail();
    }
    kvs::stream keys{const_cast<char*>(k.data()), k.size()};
    kvs::stream values{const_cast<char*>(v.data()), v.size()};
    decode_fields(key_fields_, keys, ctx.key_store_.ref(), varlen_resource);
    decode_fields(value_fields_, values, ctx.value_store_.ref(), varlen_resource);
    if(auto res = ctx.stg_->remove( *ctx.tx_, k ); ! is_ok(res)) {
        if(res == status::err_aborted_retryable) {
            ctx.state(context_state::abort);
            ctx.req_context()->status_code(res);
            return {operation_status_kind::aborted};
        }
        fail();
    }
    return {};
}

void write_partial::update_fields(
    std::vector<details::write_partial_field> const& fields,
    accessor::record_ref target,
    accessor::record_ref source
) {
    for(auto const& f : fields) {
        if (! f.updated_) continue;
        // currently assuming all the fields in source/target are nullable
        utils::copy_nullable_field(
            f.type_,
            target,
            f.target_offset_,
            f.target_nullity_offset_,
            source,
            f.update_variable_offset_,
            f.update_variable_nullity_offset_
        );
    }
}

void write_partial::decode_fields(
    std::vector<details::write_partial_field> const& fields,
    kvs::stream& stream,
    accessor::record_ref target,
    memory::lifo_paged_memory_resource* varlen_resource
) {
    for(auto&& f : fields) {
        if (f.nullable_) {
            kvs::decode_nullable(
                stream,
                f.type_,
                f.spec_,
                target,
                f.target_offset_,
                f.target_nullity_offset_,
                varlen_resource
            );
            continue;
        }
        kvs::decode(stream, f.type_, f.spec_, target, f.target_offset_, varlen_resource);
        target.set_null(f.target_nullity_offset_, false); // currently assuming fields are nullable and f.nullity_offset_ is valid even if f.nullable_ is false
    }
}

void write_partial::check_length_and_extend_buffer(
    bool from_variables,
    write_partial_context& ,
    std::vector<details::write_partial_field> const& fields,
    data::aligned_buffer& buffer,
    accessor::record_ref source
) {
    kvs::stream null_stream{};
    encode_fields(from_variables, fields, null_stream, source);
    if (null_stream.length() > buffer.size()) {
        buffer.resize(null_stream.length());
    }
}

std::vector<details::write_partial_field> write_partial::create_fields(
    write_kind,
    yugawara::storage::index const& idx,
    sequence_view<key const> keys, // keys to identify the updated record, possibly part of idx.keys()
    sequence_view<column const> columns, // columns to be updated
    processor_info const& info,
    operator_base::block_index_type block_index,
    bool key
) {
    std::vector<details::write_partial_field> ret{};
    using variable = takatori::descriptor::variable;
    yugawara::binding::factory bindings{};
    auto& block = info.scopes_info()[block_index];
    std::unordered_map<variable, variable> key_dest_to_src{};
    std::unordered_map<variable, variable> column_dest_to_src{};
    for(auto&& c : keys) {
        key_dest_to_src.emplace(c.destination(), c.source());
    }
    for(auto&& c : columns) {
        column_dest_to_src.emplace(c.destination(), c.source());
    }
    if (key) {
        auto meta = create_meta(idx, true);
        ret.reserve(idx.keys().size());
        for(std::size_t i=0, n=idx.keys().size(); i<n; ++i) {
            auto&& k = idx.keys()[i];
            auto kc = bindings(k.column());
            auto t = utils::type_for(k.column().type());
            auto spec = k.direction() == relation::sort_direction::ascendant ?
                kvs::spec_key_ascending : kvs::spec_key_descending;
            if (key_dest_to_src.count(kc) == 0) {
                fail(); // TODO update by non-unique keys
            }
            auto&& var = key_dest_to_src.at(kc);
            std::size_t source_offset{block.value_map().at(var).value_offset()};
            std::size_t source_nullity_offset{block.value_map().at(var).nullity_offset()};
            bool updated = false;
            std::size_t update_source_offset{npos};
            std::size_t update_source_nullity_offset{npos};
            if (column_dest_to_src.count(kc) != 0) {
                updated = true;
                auto&& src = column_dest_to_src.at(kc);
                update_source_offset = block.value_map().at(src).value_offset();
                update_source_nullity_offset = block.value_map().at(src).nullity_offset();
            }
            ret.emplace_back(
                t,
                source_offset,
                source_nullity_offset,
                meta->value_offset(i),
                meta->nullity_offset(i),
                k.column().criteria().nullity().nullable(),
                spec,
                updated,
                update_source_offset,
                update_source_nullity_offset
            );
        }
        return ret;
    }
    auto meta = create_meta(idx, false);
    ret.reserve(idx.values().size());
    for(std::size_t i=0, n=idx.values().size(); i<n; ++i) {
        auto&& v = idx.values()[i];
        auto b = bindings(v);
        auto& c = static_cast<yugawara::storage::column const&>(v);
        auto t = utils::type_for(c.type());
        bool updated = false;
        std::size_t update_source_offset{npos};
        std::size_t update_source_nullity_offset{npos};
        if (column_dest_to_src.count(b) != 0) {
            updated = true;
            auto&& src = column_dest_to_src.at(b);
            update_source_offset = block.value_map().at(src).value_offset();
            update_source_nullity_offset = block.value_map().at(src).nullity_offset();
        }
        ret.emplace_back(
            t,
            npos, // in value handling, no src variable coming from upstream
            npos, // in value handling, no src variable coming from upstream
            meta->value_offset(i),
            meta->nullity_offset(i),
            c.criteria().nullity().nullable(),
            kvs::spec_value,
            updated,
            update_source_offset,
            update_source_nullity_offset
        );
    }
    return ret;
}

maybe_shared_ptr<meta::record_meta> write_partial::create_meta(yugawara::storage::index const& idx, bool for_key) {
    std::vector<meta::field_type> types{};
    boost::dynamic_bitset<std::uint64_t> nullities{};
    if (for_key) {
        for(auto&& k : idx.keys()) {
            types.emplace_back(utils::type_for(k.column().type()));
            nullities.push_back(true);
        }
    } else {
        for(auto&& v : idx.values()) {
            types.emplace_back(utils::type_for(static_cast<yugawara::storage::column const&>(v).type()));
            nullities.push_back(true);
        }
    }
    return std::make_shared<meta::record_meta>(std::move(types), std::move(nullities));
}

void write_partial::encode_fields(
    bool from_variable,
    std::vector<details::write_partial_field> const& fields,
    kvs::stream& target,
    accessor::record_ref source
) {
    for(auto const& f : fields) {
        std::size_t offset = from_variable ? f.variable_offset_ : f.target_offset_;
        std::size_t nullity_offset = from_variable ? f.variable_nullity_offset_ : f.target_nullity_offset_;
        if(f.nullable_) {
            kvs::encode_nullable(source, offset, nullity_offset, f.type_, f.spec_, target);
        } else {
            kvs::encode(source, offset, f.type_, f.spec_, target);
        }
    }
}

maybe_shared_ptr<meta::record_meta> const& write_partial::value_meta() const noexcept {
    return value_meta_;
}

maybe_shared_ptr<meta::record_meta> const& write_partial::key_meta() const noexcept {
    return key_meta_;
}

void write_partial::finish(abstract::task_context*) {
    //no-op
}

std::string_view write_partial::storage_name() const noexcept {
    return storage_name_;
}

operator_kind write_partial::kind() const noexcept {
    return operator_kind::write_partial;
}

operation_status write_partial::operator()(write_partial_context& ctx) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    // find update target and fill ctx.key_store_ and ctx.value_store_
    if(auto res = find_record_and_extract(ctx); !res) {
        return res;
    }

    // update fields in key_store_/value_store_ with values from scope variable
    update_record(ctx);

    // encode values from key_store_/value_store_ and send to kvs
    if(auto res = encode_and_put(ctx); !res) {
        return res;
    }
    return {};
}

operation_status write_partial::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<write_partial_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<write_partial_context>(
            index(),
            ctx.block_scope(block_index()),
            ctx.database()->get_storage(storage_name()),
            ctx.transaction(),
            key_meta_,
            value_meta_,
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p);
}

write_partial::write_partial(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    std::string_view storage_name,
    yugawara::storage::index const& idx,
    sequence_view<key const> keys,
    sequence_view<column const> columns
) :
    write_partial(
        index,
        info,
        block_index,
        kind,
        storage_name,
        create_fields(kind, idx, keys, columns, info, block_index, true),
        create_fields(kind, idx, keys, columns, info, block_index, false),
        create_meta(idx, true),
        create_meta(idx, false)
    )
{}

write_partial::write_partial(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    write_kind kind,
    std::string_view storage_name,
    std::vector<details::write_partial_field> key_fields,
    std::vector<details::write_partial_field> value_fields,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta
) :
    record_operator(index, info, block_index),
    kind_(kind),
    storage_name_(storage_name),
    key_fields_(std::move(key_fields)),
    value_fields_(std::move(value_fields)),
    key_meta_(std::move(key_meta)),
    value_meta_(std::move(value_meta))
{}

}
