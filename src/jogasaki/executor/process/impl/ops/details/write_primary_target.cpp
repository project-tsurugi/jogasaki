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
#include <takatori/util/exception.h>
#include <yugawara/binding/factory.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/error.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/utils.h>

namespace jogasaki::executor::process::impl::ops::details {

using takatori::util::maybe_shared_ptr;
using takatori::util::throw_exception;

status encode_fields(
    std::vector<index::field_info> const& fields,
    kvs::writable_stream& target,
    accessor::record_ref source
);

status do_encode(
    data::aligned_buffer& buf,
    std::vector<index::field_info> const& info,
    accessor::record_ref source,
    std::string_view& out
);

write_primary_target::write_primary_target(
    yugawara::storage::index const& idx,
    sequence_view<key const> keys,
    sequence_view<column const> columns,
    variable_table_info const& input_variable_info,
    variable_table_info const* host_variable_info
) :
    write_primary_target(
        idx.simple_name(),
        index::create_meta(idx, true),
        index::create_meta(idx, false),
        index::create_fields(idx, keys, input_variable_info, true, false),
        index::index_fields(idx, true),
        index::index_fields(idx, false),
        create_update_fields(idx, keys, columns, host_variable_info, input_variable_info)
    )
{}

write_primary_target::write_primary_target(
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    std::vector<index::field_info> input_keys,
    std::vector<index::field_info> extracted_keys,
    std::vector<index::field_info> extracted_values,
    std::vector<details::update_field> updates
) :
    storage_name_(storage_name),
    key_meta_(std::move(key_meta)),
    value_meta_(std::move(value_meta)),
    input_keys_(std::move(input_keys)),
    extracted_keys_(std::move(extracted_keys)),
    extracted_values_(std::move(extracted_values)),
    updates_(std::move(updates))
{}

status write_primary_target::find_record_and_remove(
    write_primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref variables,
    memory_resource* varlen_resource
) {
    std::string_view k{};
    if(auto res = find_record(ctx, tx, variables, varlen_resource, k); res != status::ok) {
        return res;
    }
    return remove_record_by_encoded_key(ctx, tx, k);
}

status write_primary_target::find_record(
    write_primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref variables,
    memory_resource* varlen_resource
) {
    std::string_view k{};
    return find_record(ctx, tx, variables, varlen_resource, k);
}

status write_primary_target::find_record(
    write_primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref variables,
    memory_resource* varlen_resource,
    std::string_view& key
) {
    if(auto res = prepare_encoded_key(ctx, variables, key); res != status::ok) {
        return res;
    }
    std::string_view v{};
    if(auto res = ctx.stg_->get(tx, key, v); res != status::ok) {
        return res;
    }
    kvs::readable_stream keys{key.data(), key.size()};
    kvs::readable_stream values{v.data(), v.size()};
    if(auto res = decode_fields(extracted_keys_, keys, ctx.key_store_.ref(), varlen_resource); res != status::ok) {
        return res;
    }
    if(auto res = decode_fields(extracted_values_, values, ctx.value_store_.ref(), varlen_resource); res != status::ok) {
        return res;
    }
    return status::ok;
}

status write_primary_target::remove_record(
    write_primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref variables
) {
    std::string_view k{};
    if(auto res = prepare_encoded_key(ctx, variables, k); res != status::ok) {
        return res;
    }
    return remove_record_by_encoded_key(ctx, tx, k);
}

status write_primary_target::remove_record_by_encoded_key(
    write_primary_context& ctx,
    transaction_context& tx,
    std::string_view key
) {
    if(auto res = ctx.stg_->remove(tx, key); res != status::ok) {
        return res;
    }
    return status::ok;
}

status write_primary_target::prepare_encoded_key(
    write_primary_context& ctx,
    accessor::record_ref source,
    std::string_view& out
) const {
    if(auto res = do_encode(ctx.key_buf_, input_keys_, source, out); res != status::ok) {
        return res;
    }
    ctx.key_len_ = out.size();
    return status::ok;
}

status do_encode(
    data::aligned_buffer& buf,
    std::vector<index::field_info> const& info,
    accessor::record_ref source,
    std::string_view& out
) {
    std::size_t length{};
    for(int loop = 0; loop < 2; ++loop) { // if first trial overflows `buf`, extend it and retry
        kvs::writable_stream keys{buf.data(), buf.capacity(), loop == 0};
        if(auto res = encode_fields(info, keys, source); res != status::ok) {
            return res;
        }
        length = keys.size();
        bool fit = length <= buf.capacity();
        buf.resize(length);
        if (loop == 0) {
            if (fit) {
                break;
            }
            buf.resize(0); // set data size 0 and start from beginning
        }
    }
    out = static_cast<std::string_view>(buf);
    return status::ok;
}
status write_primary_target::encode_and_put(
    write_primary_context& ctx,
    transaction_context& tx,
    kvs::put_option opt
) const {
    std::string_view k{};
    std::string_view v{};
    if(auto res = do_encode(ctx.key_buf_, extracted_keys_, ctx.key_store_.ref(), k); res != status::ok) {
        return res;
    }
    ctx.key_len_ = k.size();
    if(auto res = do_encode(ctx.value_buf_, extracted_values_, ctx.value_store_.ref(), v); res != status::ok) {
        return res;
    }
    if(auto res = ctx.stg_->put(tx, k, v, opt); res != status::ok) {
        return res;
    }
    return status::ok;
}

void write_primary_target::update_record(
    write_primary_context& ctx,
    accessor::record_ref input_variables,
    accessor::record_ref host_variables
) {
    for(auto const& f : updates_) {
        // assuming intermediate fields are nullable. Nullability check is done on encoding.
        auto target = f.key_ ? ctx.key_store_.ref() : ctx.value_store_.ref();
        utils::copy_nullable_field(
            f.type_,
            target,
            f.target_offset_,
            f.target_nullity_offset_,
            f.source_external_ ? host_variables : input_variables,
            f.source_offset_,
            f.source_nullity_offset_
        );
    }
}

status write_primary_target::decode_fields(
    std::vector<index::field_info> const& fields,
    kvs::readable_stream& stream,
    accessor::record_ref target,
    memory::lifo_paged_memory_resource* varlen_resource
) const {
    for(auto&& f : fields) {
        if (f.nullable_) {
            if(auto res = kvs::decode_nullable(
                    stream,
                    f.type_,
                    f.spec_,
                    target,
                    f.offset_,
                    f.nullity_offset_,
                    varlen_resource
                ); res != status::ok) {
                return res;
            }
            continue;
        }
        if(auto res = kvs::decode(stream, f.type_, f.spec_, target, f.offset_, varlen_resource); res != status::ok) {
            return res;
        }
        target.set_null(f.nullity_offset_, false); // currently assuming fields are nullable and
        // f.nullity_offset_ is valid even if f.nullable_
        // is false
    }
    return status::ok;
}

status encode_fields(
    std::vector<index::field_info> const& fields,
    kvs::writable_stream& target,
    accessor::record_ref source
) {
    for(auto const& f : fields) {
        if(f.nullable_) {
            if(auto res = kvs::encode_nullable(source, f.offset_, f.nullity_offset_, f.type_, f.spec_, target);
                res != status::ok) {
                return res;
            }
        } else {
            if(source.is_null(f.nullity_offset_)) {
                VLOG_LP(log_error) << "Null assigned for non-nullable field.";
                return status::err_integrity_constraint_violation;
            }
            if(auto res = kvs::encode(source, f.offset_, f.type_, f.spec_, target); res != status::ok) {
                return res;
            }
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

std::vector<details::update_field> write_primary_target::create_update_fields(
    yugawara::storage::index const& idx,
    sequence_view<key const> keys, // keys to identify the updated record, possibly part of idx.keys()
    sequence_view<column const> columns, // columns to be updated
    variable_table_info const* host_variable_info,
    variable_table_info const& input_variable_info
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
            auto t = utils::type_for(k.column().type());
            if (key_dest_to_src.count(kc) == 0) {
                throw_exception(std::logic_error{""}); // TODO update by non-unique keys
            }
            if (column_dest_to_src.count(kc) != 0) {
                auto&& src = column_dest_to_src.at(kc);
                auto [os, nos, b] = resolve_variable_offsets(input_variable_info, host_variable_info, src);
                ret.emplace_back(
                    t,
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
        auto t = utils::type_for(c.type());
        if (column_dest_to_src.count(b) != 0) {
            auto&& src = column_dest_to_src.at(b);
            auto [os, nos, src_is_external ] = resolve_variable_offsets(input_variable_info, host_variable_info, src);
            ret.emplace_back(
                t,
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

maybe_shared_ptr<meta::record_meta> const& write_primary_target::key_meta() const noexcept {
    return key_meta_;
}

maybe_shared_ptr<meta::record_meta> const& write_primary_target::value_meta() const noexcept {
    return value_meta_;
}

std::string_view write_primary_target::storage_name() const noexcept {
    return storage_name_;
}

bool write_primary_target::updates_key() const noexcept {
    return std::any_of(updates_.begin(), updates_.end(), [](auto f) {
        return f.key_;
    });
}

}
