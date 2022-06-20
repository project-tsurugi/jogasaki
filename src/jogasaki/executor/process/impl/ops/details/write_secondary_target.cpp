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
#include "write_secondary_target.h"

#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/kvs/coder.h>
#include <jogasaki/error.h>
#include <jogasaki/logging.h>
#include <jogasaki/kvs/writable_stream.h>
#include "write_secondary_context.h"

namespace jogasaki::executor::process::impl::ops::details {

status details::write_secondary_target::encode_key(
    write_secondary_context& ctx,
    accessor::record_ref source_key,
    accessor::record_ref source_value,
    std::string_view primary_key,
    std::string_view& out
) const {
    std::size_t length{};
    auto& buf = ctx.key_buf_;
    for(int loop = 0; loop < 2; ++loop) { // if first trial overflows `buf`, extend it and retry
        kvs::writable_stream s{buf.data(), buf.size(), loop == 0};
        for(auto&& f : secondary_key_fields_) {
            auto src = f.key_ ? source_key : source_value;
            if (f.nullable_) {
                if(auto res = kvs::encode_nullable(src, f.offset_, f.nullity_offset_, f.type_, f.spec_, s);
                    res != status::ok) {
                    return res;
                }
            } else {
                if(auto res = kvs::encode(src, f.offset_, f.type_, f.spec_, s); res != status::ok) {
                    return res;
                }
            }
        }
        if (auto res = s.write(primary_key.data(), primary_key.size()); res != status::ok) {
            return res;
        }
        if (loop == 0) {
            length = s.size();
            if (length <= buf.size()) {
                break;
            }
            buf.resize(length);
        }
    }
    out = {static_cast<char*>(buf.data()), length};
    return status::ok;
}

status details::write_secondary_target::encode_and_put(
    write_secondary_context& ctx,
    transaction_context& tx,
    accessor::record_ref source_key,
    accessor::record_ref source_value,
    std::string_view primary_key
) const {
    std::string_view k{};
    if(auto res = encode_key(ctx, source_key, source_value, primary_key, k); res != status::ok) {
        return res;
    }
    if(auto res = ctx.stg_->put(tx, k, {}, kvs::put_option::create_or_update); res != status::ok) {
        VLOG(log_error) << "upserting to secondary index failed: " << res;
        return res;
    }
    return status::ok;
}

status details::write_secondary_target::encode_and_remove(
    write_secondary_context& ctx,
    transaction_context& tx,
    accessor::record_ref source_key,
    accessor::record_ref source_value,
    std::string_view primary_key
) const {
    std::string_view k{};
    if(auto res = encode_key(ctx, source_key, source_value, primary_key, k); res != status::ok) {
        return res;
    }
    if(auto res = ctx.stg_->remove(tx, k); ! is_ok(res)) {
        if (res == status::not_found) {
            VLOG(log_warning) << "inconsistent secondary index - entry not found";
        } else {
            VLOG(log_error) << "removing from secondary index failed: " << res;
        }
        return res;
    }
    return status::ok;
}

}