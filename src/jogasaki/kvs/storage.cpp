/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "storage.h"

#include <memory>

#include <sharksfin/Slice.h>
#include <sharksfin/StatusCode.h>
#include <sharksfin/api.h>

#include <jogasaki/kvs/error.h>
#include <jogasaki/lob/lob_id.h>

#include "iterator.h"
#include "transaction.h"


namespace jogasaki::kvs {

using sharksfin::Slice;
using sharksfin::StatusCode;

status storage::delete_storage() {
    auto res = sharksfin::storage_delete(handle_);
    return resolve(res);
}

status storage::content_get(
    transaction& tx,
    std::string_view key,
    std::string_view& value
) {
    Slice v{};
    StatusCode res = sharksfin::content_get(
        tx.handle(),
        handle_,
        key,
        &v
    );
    if (res == StatusCode::OK) {
        value = v.to_string_view();
        return status::ok;
    }
    if (res == StatusCode::NOT_FOUND) {
        return status::not_found;
    }
    return resolve(res);
}

status storage::content_put(
    transaction& tx,
    std::string_view key,
    std::string_view value,
    put_option option,
    std::vector<lob::lob_id_type> const& lobs
) {
    auto res = sharksfin::content_put_with_blobs(
        tx.handle(),
        handle_,
        key,
        value,
        lobs.empty() ? nullptr : lobs.data(),
        lobs.empty() ? 0 : lobs.size(),
        static_cast<sharksfin::PutOperation>(option)
    );
    if (res == StatusCode::NOT_FOUND) {
        if (option == put_option::update) {
            return status::not_found;
        }
        return status::ok;
    }
    if (res == StatusCode::ALREADY_EXISTS) {
        if (option == put_option::create) {
            return status::already_exists;
        }
        return status::ok;
    }
    if (res == StatusCode::ERR_ILLEGAL_OPERATION) {
        return status::err_write_operation_by_rtx;
    }
    return resolve(res);
}

status storage::content_delete(
    transaction& tx,
    std::string_view key
) {
    auto res = sharksfin::content_delete(
        tx.handle(),
        handle_,
        key);
    if (res == StatusCode::ERR_ILLEGAL_OPERATION) {
        return status::err_write_operation_by_rtx;
    }
    return resolve(res);
}

status storage::content_scan(transaction &tx,
    std::string_view begin_key, end_point_kind begin_kind,
    std::string_view end_key, end_point_kind end_kind,
    std::unique_ptr<iterator>& it,
    std::size_t limit,
    bool reverse
) {
    sharksfin::IteratorHandle handle{};
    auto res = sharksfin::content_scan(
        tx.handle(),
        handle_,
        sharksfin::Slice(begin_key), kind(begin_kind),
        sharksfin::Slice(end_key), kind(end_kind), &handle,
        limit,
        reverse
    );
    if(res == sharksfin::StatusCode::OK) {
        it = std::make_unique<iterator>(handle);
        return status::ok;
    }
    return resolve(res);
}

storage::~storage() noexcept {
    sharksfin::storage_dispose(handle_);
}

storage::storage(sharksfin::StorageHandle handle) noexcept: handle_(handle) {}

sharksfin::StorageHandle storage::handle() const noexcept {
    return handle_;
}

sharksfin::EndPointKind storage::kind(end_point_kind k) {
    return static_cast<sharksfin::EndPointKind>(static_cast<std::uint32_t>(k));
}

status storage::set_options(sharksfin::StorageOptions const& options) {
    return resolve(sharksfin::storage_set_options(handle_, options));
}

status storage::get_options(sharksfin::StorageOptions& options) {
    return resolve(sharksfin::storage_get_options(handle_, options));
}

end_point_kind adjust_endpoint_kind(bool use_secondary, kvs::end_point_kind endpoint){
    if (use_secondary) {
        if (endpoint == kvs::end_point_kind::inclusive) {
            return kvs::end_point_kind::prefixed_inclusive;
        }
        if (endpoint == kvs::end_point_kind::exclusive) {
            return kvs::end_point_kind::prefixed_exclusive;
        }
    }
    return endpoint;
}

std::ostream& operator<<(std::ostream& os, end_point_kind kind) {
    switch(kind) {
        case end_point_kind::unbound:
            os << "unbound";
            break;
        case end_point_kind::inclusive:
            os << "inclusive";
            break;
        case end_point_kind::exclusive:
            os << "exclusive";
            break;
        case end_point_kind::prefixed_inclusive:
            os << "prefixed_inclusive";
            break;
        case end_point_kind::prefixed_exclusive:
            os << "prefixed_exclusive";
            break;
        default:
            os << "unknown";
            break;
    }
    return os;
}

} // namespace jogasaki::kvs
