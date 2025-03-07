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
#include "primary_target.h"

#include <memory>
#include <ostream>
#include <utility>
#include <vector>
#include <glog/logging.h>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/datastore/assign_lob_id.h>
#include <jogasaki/datastore/register_lob.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/index/primary_context.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/lob/lob_id.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/utils/handle_encode_errors.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/modify_status.h>

namespace jogasaki::index {

using takatori::util::maybe_shared_ptr;

status encode_fields(
    primary_target::field_mapping_type const& fields,
    kvs::writable_stream& target,
    accessor::record_ref source
);

status do_encode(
    data::aligned_buffer& buf,
    primary_target::field_mapping_type const& info,
    accessor::record_ref source,
    std::string_view& out
);

primary_target::primary_target(
    std::string_view storage_name,
    maybe_shared_ptr<meta::record_meta> key_meta,
    maybe_shared_ptr<meta::record_meta> value_meta,
    primary_target::field_mapping_type input_keys,
    primary_target::field_mapping_type extracted_keys,
    primary_target::field_mapping_type extracted_values
) :
    storage_name_(storage_name),
    key_meta_(std::move(key_meta)),
    value_meta_(std::move(value_meta)),
    input_keys_(std::move(input_keys)),
    extracted_keys_(std::move(extracted_keys)),
    extracted_values_(std::move(extracted_values))
{}

status primary_target::encode_find_remove(
    primary_context& ctx,
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

status primary_target::encode_find(
    primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref key,
    memory_resource* varlen_resource,
    accessor::record_ref dest_key,
    accessor::record_ref dest_value
) {
    std::string_view k{};
    return encode_find(ctx, tx, key, varlen_resource, dest_key, dest_value, k);
}

status primary_target::find_by_encoded_key(
    primary_context& ctx,
    transaction_context& tx,
    std::string_view encoded_key,
    memory_resource* varlen_resource,
    accessor::record_ref dest_key,
    accessor::record_ref dest_value
) {
    std::string_view v{};
    if(auto res = ctx.stg_->content_get(tx, encoded_key, v); res != status::ok) {
        utils::modify_concurrent_operation_status(tx, res, false);
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
status primary_target::encode_find(
    primary_context& ctx,
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
    return find_by_encoded_key(ctx, tx, encoded_key, varlen_resource, dest_key, dest_value);
}

status primary_target::encode_remove(
    primary_context& ctx,
    transaction_context& tx,
    accessor::record_ref key
) {
    std::string_view k{};
    if(auto res = prepare_encoded_key(ctx, key, k); res != status::ok) {
        return res;
    }
    return remove_by_encoded_key(ctx, tx, k);
}

status primary_target::remove_by_encoded_key(
    primary_context& ctx,
    transaction_context& tx,
    std::string_view encoded_key
) {
    if(auto res = ctx.stg_->content_delete(tx, encoded_key); res != status::ok) {
        handle_kvs_errors(*ctx.req_context(), res);
        return res;
    }
    return status::ok;
}

status primary_target::prepare_encoded_key(
    primary_context& ctx,
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
    primary_target::field_mapping_type const& info,
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

template<class T>
status resolve_lob_field(
    request_context& context,
    accessor::record_ref rec,
    index::field_info const& field,
    transaction_context* tx,
    std::vector<lob::lob_id_type>& lobs
) {
    if (rec.is_null(field.nullity_offset_)) {
        return status::ok;
    }
    auto ref = rec.get_reference<T>(field.offset_);
    if (ref.kind() == lob::lob_reference_kind::resolved) {
        return status::ok;
    }
    lob::lob_id_type id{};
    std::shared_ptr<error::error_info> error{};
    if (auto res = datastore::assign_lob_id(ref, tx, id, error); res != status::ok) {
        error::set_error_info(context, error);
        return res;
    }
    rec.set_value(field.offset_, T{id, lob::lob_data_provider::datastore});
    lobs.emplace_back(id);
    return status::ok;
}

status resolve_fields(
    request_context& context,
    accessor::record_ref rec,
    std::vector<index::field_info> const& fields,
    std::vector<lob::lob_id_type>& lobs
) {
    auto tx = context.transaction();
    for (auto&& f : fields) {
        if (f.type_.kind() == meta::field_type_kind::blob) {
            if (auto res = resolve_lob_field<lob::blob_reference>(context, rec, f, tx.get(), lobs); res != status::ok) {
                return res;
            }
        } else if (f.type_.kind() == meta::field_type_kind::clob) {
            if (auto res = resolve_lob_field<lob::clob_reference>(context, rec, f, tx.get(), lobs); res != status::ok) {
                return res;
            }
        }
    }
    return status::ok;
}

status primary_target::encode_put(
    primary_context& ctx,
    transaction_context& tx,
    kvs::put_option opt,
    accessor::record_ref key_record,
    accessor::record_ref value_record,
    std::string_view& encoded_key
) {
    std::vector<lob::lob_id_type> lobs{};
    if (ctx.req_context()) {  // some testcase does not set req_context
        if (auto res = resolve_fields(*ctx.req_context(), value_record, extracted_values_, lobs); res != status::ok) {
            // error info set by resolve_fields
            return res;
        }
    }
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
    if(auto res = ctx.stg_->content_put(tx, k, v, opt, lobs); res != status::ok) {
        handle_kvs_errors(*ctx.req_context(), res);
        return res;
    }
    encoded_key = k;
    return status::ok;
}

status primary_target::decode_fields(
    primary_target::field_mapping_type const& fields,
    kvs::readable_stream& stream,
    accessor::record_ref target,
    memory::lifo_paged_memory_resource* varlen_resource
) const {
    for(auto&& f : fields) {
        kvs::coding_context ctx{};
        if (f.nullable_) {
            if(auto res = kvs::decode_nullable(
                    stream,
                    f.type_,
                    f.spec_,
                    ctx,
                    target,
                    f.offset_,
                    f.nullity_offset_,
                    varlen_resource
                ); res != status::ok) {
                return res;
            }
            continue;
        }
        if(auto res = kvs::decode(stream, f.type_, f.spec_, ctx, target, f.offset_, varlen_resource); res != status::ok) {
            return res;
        }
        target.set_null(f.nullity_offset_, false); // currently assuming fields are nullable and
        // f.nullity_offset_ is valid even if f.nullable_ is false
    }
    return status::ok;
}

status encode_fields(
    primary_target::field_mapping_type const& fields,
    kvs::writable_stream& target,
    accessor::record_ref source
) {
    for(auto const& f : fields) {
        kvs::coding_context ctx{};
        ctx.coding_for_write(true);
        if(f.nullable_) {
            if(auto res = kvs::encode_nullable(source, f.offset_, f.nullity_offset_, f.type_, f.spec_, ctx, target);
                res != status::ok) {
                return res;
            }
        } else {
            if(source.is_null(f.nullity_offset_)) {
                VLOG_LP(log_error) << "Null assigned for non-nullable field.";
                return status::err_integrity_constraint_violation;
            }
            if(auto res = kvs::encode(source, f.offset_, f.type_, f.spec_, ctx, target); res != status::ok) {
                return res;
            }
        }
    }
    return status::ok;
}


maybe_shared_ptr<meta::record_meta> const& primary_target::key_meta() const noexcept {
    return key_meta_;
}

maybe_shared_ptr<meta::record_meta> const& primary_target::value_meta() const noexcept {
    return value_meta_;
}

std::string_view primary_target::storage_name() const noexcept {
    return storage_name_;
}

}  // namespace jogasaki::index
