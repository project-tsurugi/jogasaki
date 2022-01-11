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
#include "write_primary_target.h"

#include <vector>

#include <takatori/relation/write.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/logging.h>
#include <jogasaki/error.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::executor::process::impl::ops::details {

using takatori::util::maybe_shared_ptr;

write_partial_field::write_partial_field(
    meta::field_type type,
    std::size_t variable_offset,
    std::size_t variable_nullity_offset,
    std::size_t target_offset,
    std::size_t target_nullity_offset,
    bool nullable,
    kvs::coding_spec spec,
    bool updated,
    std::size_t update_variable_offset,
    std::size_t update_variable_nullity_offset,
    bool update_variable_is_external
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
    update_variable_nullity_offset_(update_variable_nullity_offset),
    update_variable_is_external_(update_variable_is_external)
{}

write_primary_target::write_primary_target(
    std::string_view storage_name,
    yugawara::storage::index const& idx,
    sequence_view<key const> keys,
    sequence_view<column const> columns,
    variable_table_info const* input_variable_info,
    variable_table_info const* host_variable_info
) :
    write_primary_target(
        storage_name,
        create_fields(idx, keys, columns, host_variable_info, *input_variable_info, true),
        create_fields(idx, keys, columns, host_variable_info, *input_variable_info, false),
        create_meta(idx, true),
        create_meta(idx, false)
    )
{}

write_primary_target::write_primary_target(
    std::string_view storage_name,
    std::vector<details::write_partial_field> key_fields,
    std::vector<details::write_partial_field> value_fields,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta
) :
    storage_name_(storage_name),
    key_fields_(std::move(key_fields)),
    value_fields_(std::move(value_fields)),
    key_meta_(std::move(key_meta)),
    value_meta_(std::move(value_meta))
{}

status write_primary_target::find_record_and_extract(
    write_primary_context& ctx,
    kvs::transaction& tx,
    accessor::record_ref variables,
    memory_resource* varlen_resource
) {
    std::string_view k{};
    if(auto res = prepare_encoded_key(ctx, variables, k); res != status::ok) {
        return res;
    }
    std::string_view v{};
    if(auto res = ctx.stg_->get( tx, k, v ); ! is_ok(res)) {
        if(res == status::err_aborted_retryable) {
            return res;
        }
        // The update target has been identified on the upstream operator such as find,
        // so this lookup must be successful. If the control reaches here, it's internal error.
        fail();
    }
    kvs::readable_stream keys{k.data(), k.size()};
    kvs::readable_stream values{v.data(), v.size()};
    decode_fields(key_fields_, keys, ctx.key_store_.ref(), varlen_resource);
    decode_fields(value_fields_, values, ctx.value_store_.ref(), varlen_resource);
    if(auto res = ctx.stg_->remove( tx, k ); ! is_ok(res)) {
        if(res == status::err_aborted_retryable) {
            return res;
        }
        fail();
    }
    return status::ok;
}

status write_primary_target::prepare_encoded_key(write_primary_context& ctx, accessor::record_ref source, std::string_view& out) const {
    // calculate length first, and then put
    if(auto res = check_length_and_extend_buffer(true, ctx, key_fields_, ctx.key_buf_, source); res != status::ok) {
        return res;
    }
    kvs::writable_stream keys{ctx.key_buf_.data(), ctx.key_buf_.size()};
    if(auto res = encode_fields(true, key_fields_, keys, source); res != status::ok) {
        return res;
    }
    out = {keys.data(), keys.size()};
    return status::ok;
}

status write_primary_target::encode_and_put(write_primary_context& ctx, kvs::transaction& tx) const {
    auto key_source = ctx.key_store_.ref();
    auto val_source = ctx.value_store_.ref();
    // calculate length first, then put
    if(auto res = check_length_and_extend_buffer(false, ctx, key_fields_, ctx.key_buf_, key_source); res != status::ok) {
        return res;
    }
    if(auto res = check_length_and_extend_buffer(false, ctx, value_fields_, ctx.value_buf_, val_source); res != status::ok) {
        return res;
    }
    kvs::writable_stream keys{ctx.key_buf_.data(), ctx.key_buf_.size()};
    kvs::writable_stream values{ctx.value_buf_.data(), ctx.value_buf_.size()};
    if(auto res = encode_fields(false, key_fields_, keys, key_source); res != status::ok) {
        return res;
    }
    if(auto res = encode_fields(false, value_fields_, values, val_source); res != status::ok) {
        return res;
    }
    if(auto res = ctx.stg_->put(
            tx,
            {keys.data(), keys.size()},
            {values.data(), values.size()}
        ); is_error(res)) {
        if(res == status::err_aborted_retryable) {
            return res;
        }
        // updating already found record, so err_not_found should never happen
        fail();
    }
    return status::ok;
}

void write_primary_target::update_record(
    write_primary_context& ctx,
    accessor::record_ref input_variables,
    accessor::record_ref host_variables
) {
    update_fields(key_fields_, ctx.key_store_.ref(), input_variables, host_variables);
    update_fields(value_fields_, ctx.value_store_.ref(), input_variables, host_variables);
}

void write_primary_target::update_fields(
    std::vector<details::write_partial_field> const& fields,
    accessor::record_ref target,
    accessor::record_ref input_variables,
    accessor::record_ref host_variables
) const {
    for(auto const& f : fields) {
        if (! f.updated_) continue;
        // assuming intermediate fields are nullable. Nullability check is done on encoding.
        utils::copy_nullable_field(
            f.type_,
            target,
            f.target_offset_,
            f.target_nullity_offset_,
            f.update_variable_is_external_ ? host_variables : input_variables,
            f.update_variable_offset_,
            f.update_variable_nullity_offset_
        );
    }
}

void write_primary_target::decode_fields(
    std::vector<details::write_partial_field> const& fields,
    kvs::readable_stream& stream,
    accessor::record_ref target,
    memory::lifo_paged_memory_resource* varlen_resource
) const {
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
        target.set_null(f.target_nullity_offset_, false); // currently assuming fields are nullable and
        // f.nullity_offset_ is valid even if f.nullable_
        // is false
    }
}

status write_primary_target::check_length_and_extend_buffer(
    bool from_variables,
    write_primary_context& ,
    std::vector<details::write_partial_field> const& fields,
    data::aligned_buffer& buffer,
    accessor::record_ref source
) const {
    kvs::writable_stream null_stream{};
    if(auto res = encode_fields(from_variables, fields, null_stream, source); res != status::ok) {
        return res;
    }
    if (null_stream.size() > buffer.size()) {
        buffer.resize(null_stream.size());
    }
    return status::ok;
}

status write_primary_target::encode_fields(
    bool from_variable,
    std::vector<details::write_partial_field> const& fields,
    kvs::writable_stream& target,
    accessor::record_ref source
) const {
    for(auto const& f : fields) {
        std::size_t offset = from_variable ? f.variable_offset_ : f.target_offset_;
        std::size_t nullity_offset = from_variable ? f.variable_nullity_offset_ : f.target_nullity_offset_;
        if(f.nullable_) {
            kvs::encode_nullable(source, offset, nullity_offset, f.type_, f.spec_, target);
        } else {
            if(source.is_null(nullity_offset)) {
                VLOG(log_error) << "Null assigned for non-nullable field.";
                return status::err_integrity_constraint_violation;
            }
            kvs::encode(source, offset, f.type_, f.spec_, target);
        }
    }
    return status::ok;
}

std::tuple<std::size_t, std::size_t, bool> resolve_variable_offsets(
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


std::vector<details::write_partial_field> write_primary_target::create_fields(
    yugawara::storage::index const& idx,
    sequence_view<key const> keys, // keys to identify the updated record, possibly part of idx.keys()
    sequence_view<column const> columns, // columns to be updated
    variable_table_info const* host_variable_info,
    variable_table_info const& input_variable_info,
    bool key
) {
    std::vector<details::write_partial_field> ret{};
    yugawara::binding::factory bindings{};
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
            std::size_t source_offset{input_variable_info.at(var).value_offset()};
            std::size_t source_nullity_offset{input_variable_info.at(var).nullity_offset()};
            bool updated = false;
            std::size_t update_source_offset{npos};
            std::size_t update_source_nullity_offset{npos};
            bool update_src_is_external{false};
            if (column_dest_to_src.count(kc) != 0) {
                updated = true;
                auto&& src = column_dest_to_src.at(kc);
                auto [os, nos, b] = resolve_variable_offsets(input_variable_info, host_variable_info, src);
                update_source_offset = os;
                update_source_nullity_offset = nos;
                update_src_is_external = b;
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
                update_source_nullity_offset,
                update_src_is_external
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
        bool update_src_is_external{false};
        if (column_dest_to_src.count(b) != 0) {
            updated = true;
            auto&& src = column_dest_to_src.at(b);
            auto [os, nos, src_is_external ] = resolve_variable_offsets(input_variable_info, host_variable_info, src);
            update_source_offset = os;
            update_source_nullity_offset = nos;
            update_src_is_external = src_is_external;
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
            update_source_nullity_offset,
            update_src_is_external
        );
    }
    return ret;
}

maybe_shared_ptr<meta::record_meta> write_primary_target::create_meta(yugawara::storage::index const& idx, bool for_key) {
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

}
