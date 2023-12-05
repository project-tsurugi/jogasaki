/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <jogasaki/error.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/field_types.h>
#include <jogasaki/utils/handle_encode_errors.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/handle_kvs_errors.h>

namespace jogasaki::executor::process::impl::ops::details {

using takatori::util::maybe_shared_ptr;
using takatori::util::throw_exception;

status encode_fields(
    write_primary_target::field_mapping_type const& fields,
    kvs::writable_stream& target,
    accessor::record_ref source
);

status do_encode(
    data::aligned_buffer& buf,
    write_primary_target::field_mapping_type const& info,
    accessor::record_ref source,
    std::string_view& out
);

write_primary_target::write_primary_target(
    yugawara::storage::index const& idx,
    sequence_view<key const> keys,
    variable_table_info const& input_variable_info
) :
    write_primary_target(
        idx.simple_name(),
        index::create_meta(idx, true),
        index::create_meta(idx, false),
        index::create_fields(idx, keys, input_variable_info, true, false),
        index::index_fields(idx, true),
        index::index_fields(idx, false)
    )
{}

write_primary_target::write_primary_target(
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    write_primary_target::field_mapping_type input_keys,
    write_primary_target::field_mapping_type extracted_keys,
    write_primary_target::field_mapping_type extracted_values
) :
    storage_name_(storage_name),
    key_meta_(std::move(key_meta)),
    value_meta_(std::move(value_meta)),
    input_keys_(std::move(input_keys)),
    extracted_keys_(std::move(extracted_keys)),
    extracted_values_(std::move(extracted_values))
{}

status write_primary_target::encode_find_remove(
    write_primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref key,
    memory_resource* varlen_resource,
    accessor::record_ref dest_key,
    accessor::record_ref dest_value
) {
    std::string_view k{};
    if(auto res = encode_find(ctx, tx, key, varlen_resource, dest_key, dest_value, k); res != status::ok) {
        return res;
    }
    return remove_by_encoded_key(ctx, tx, k);
}

status write_primary_target::encode_find(
    write_primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref key,
    memory_resource* varlen_resource,
    accessor::record_ref dest_key,
    accessor::record_ref dest_value
) {
    std::string_view k{};
    return encode_find(ctx, tx, key, varlen_resource, dest_key, dest_value, k);
}

status write_primary_target::encode_find(
    write_primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref key,
    memory_resource* varlen_resource,
    accessor::record_ref dest_key,
    accessor::record_ref dest_value,
    std::string_view& encoded_key
) {
    if(auto res = prepare_encoded_key(ctx, key, encoded_key); res != status::ok) {
        handle_encode_errors(*ctx.req_context(), res);
        return res;
    }
    std::string_view v{};
    if(auto res = ctx.stg_->get(tx, encoded_key, v); res != status::ok) {
        handle_kvs_errors(*ctx.req_context(), res);
        return res;
    }
    kvs::readable_stream keys{encoded_key.data(), encoded_key.size()};
    kvs::readable_stream values{v.data(), v.size()};
    if(auto res = decode_fields(extracted_keys_, keys, dest_key, varlen_resource);
       res != status::ok) {
        handle_encode_errors(*ctx.req_context(), res);
        return res;
    }
    if(auto res = decode_fields(extracted_values_, values, dest_value, varlen_resource);
       res != status::ok) {
        handle_encode_errors(*ctx.req_context(), res);
        return res;
    }
    return status::ok;
}

status write_primary_target::encode_remove(
    write_primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref key
) {
    std::string_view k{};
    if(auto res = prepare_encoded_key(ctx, key, k); res != status::ok) {
        return res;
    }
    return remove_by_encoded_key(ctx, tx, k);
}

status write_primary_target::remove_by_encoded_key(
    write_primary_context& ctx,
    transaction_context& tx,
    std::string_view encoded_key
) {
    if(auto res = ctx.stg_->remove(tx, encoded_key); res != status::ok) {
        handle_kvs_errors(*ctx.req_context(), res);
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
        handle_encode_errors(*ctx.req_context(), res);
        return res;
    }
    ctx.key_len_ = out.size();
    return status::ok;
}

status do_encode(
    data::aligned_buffer& buf,
    write_primary_target::field_mapping_type const& info,
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

status write_primary_target::encode_put(
    write_primary_context& ctx,
    transaction_context& tx,
    kvs::put_option opt,
    accessor::record_ref key_record,
    accessor::record_ref value_record,
    std::string_view& encoded_key
) {
    std::string_view k{};
    std::string_view v{};
    if(auto res = do_encode(ctx.key_buf_, extracted_keys_, key_record, k); res != status::ok) {
        handle_encode_errors(*ctx.req_context(), res);
        return res;
    }
    ctx.key_len_ = k.size();
    if(auto res = do_encode(ctx.value_buf_, extracted_values_, value_record, v); res != status::ok) {
        handle_encode_errors(*ctx.req_context(), res);
        return res;
    }
    if(auto res = ctx.stg_->put(tx, k, v, opt); res != status::ok) {
        handle_kvs_errors(*ctx.req_context(), res);
        return res;
    }
    encoded_key = k;
    return status::ok;
}

status write_primary_target::decode_fields(
    write_primary_target::field_mapping_type const& fields,
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
        // f.nullity_offset_ is valid even if f.nullable_ is false
    }
    return status::ok;
}

status encode_fields(
    write_primary_target::field_mapping_type const& fields,
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


maybe_shared_ptr<meta::record_meta> const& write_primary_target::key_meta() const noexcept {
    return key_meta_;
}

maybe_shared_ptr<meta::record_meta> const& write_primary_target::value_meta() const noexcept {
    return value_meta_;
}

std::string_view write_primary_target::storage_name() const noexcept {
    return storage_name_;
}

}  // namespace jogasaki::executor::process::impl::ops::details
