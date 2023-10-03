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
#include "storage.h"

#include <memory>
#include <sharksfin/api.h>
#include <sharksfin/Slice.h>
#include "transaction.h"
#include "iterator.h"

#include <jogasaki/kvs/error.h>

namespace jogasaki::kvs {

using sharksfin::Slice;
using sharksfin::StatusCode;

status storage::delete_storage() {
    auto res = sharksfin::storage_delete(handle_);
    return resolve(res);
}

status storage::get(
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

status storage::put(
    transaction& tx,
    std::string_view key,
    std::string_view value,
    put_option option
) {
    auto res = sharksfin::content_put(
        tx.handle(),
        handle_,
        key,
        value,
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

status storage::remove(
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

status storage::scan(transaction &tx,
    std::string_view begin_key, end_point_kind begin_kind,
    std::string_view end_key, end_point_kind end_kind,
    std::unique_ptr<iterator>& it
) {
    sharksfin::IteratorHandle handle{};
    auto res = sharksfin::content_scan(
        tx.handle(),
        handle_,
        sharksfin::Slice(begin_key), kind(begin_kind),
        sharksfin::Slice(end_key), kind(end_kind), &handle
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
}

