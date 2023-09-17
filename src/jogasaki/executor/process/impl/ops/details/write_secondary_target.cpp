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
#include <jogasaki/logging_helper.h>
#include <jogasaki/kvs/writable_stream.h>
#include <jogasaki/utils/handle_kvs_errors.h>
#include <jogasaki/utils/handle_generic_error.h>
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
        kvs::writable_stream s{buf.data(), buf.capacity(), loop == 0};
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
        length = s.size();
        bool fit = length <= buf.capacity();
        buf.resize(length);
        if (loop == 0) {
            if (fit) {
                break;
            }
            buf.resize(0); // set data size 0 and start from beginning
        }
    }
    out = {static_cast<std::string_view>(buf)};
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
        handle_kvs_errors(*ctx.req_context(), res);
        handle_generic_error(*ctx.req_context(), res, error_code::sql_execution_exception);
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
        handle_kvs_errors(*ctx.req_context(), res);
        handle_generic_error(*ctx.req_context(), res, error_code::sql_execution_exception);
        return res;
    }
    return status::ok;
}

std::vector<details::secondary_key_field> write_secondary_target::create_fields(
    yugawara::storage::index const& idx,
    maybe_shared_ptr<meta::record_meta> const& primary_key_meta,
    maybe_shared_ptr<meta::record_meta> const& primary_value_meta
) {
    auto& table = idx.table();
    auto primary = table.owner()->find_primary_index(table);
    if(!(primary != nullptr)) throw_exception(std::logic_error{""});
    std::vector<details::secondary_key_field> ret{};
    ret.reserve(table.columns().size());
    for(auto&& k : idx.keys()) {
        bool found = false;
        for(std::size_t i=0, n=primary->keys().size(); i<n; ++i) {
            if(primary->keys().at(i) == k) {
                auto spec = k.direction() == takatori::relation::sort_direction::ascendant ?
                    kvs::spec_key_ascending : kvs::spec_key_descending;
                // pass storage spec with fields for write
                spec.storage(index::extract_storage_spec(k.column().type()));
                ret.emplace_back(
                    primary_key_meta->at(i),
                    primary_key_meta->value_offset(i),
                    primary_key_meta->nullity_offset(i),
                    k.column().criteria().nullity().nullable(),
                    spec,
                    true
                );
                found = true;
                break;
            }
        }
        if(found) continue;
        for(std::size_t i=0, n=primary->values().size(); i<n; ++i) {
            if(primary->values().at(i) == k.column()) {
                auto spec = k.direction() == takatori::relation::sort_direction::ascendant ?
                    kvs::spec_key_ascending : kvs::spec_key_descending;
                // pass storage spec with fields for write
                spec.storage(index::extract_storage_spec(k.column().type()));
                ret.emplace_back(
                    primary_value_meta->at(i),
                    primary_value_meta->value_offset(i),
                    primary_value_meta->nullity_offset(i),
                    k.column().criteria().nullity().nullable(),
                    spec,
                    false
                );
                found = true;
                break;
            }
        }
        if(found) continue;
        throw_exception(std::logic_error{""});
    }
    return ret;
}

}