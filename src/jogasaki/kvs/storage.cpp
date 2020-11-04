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
#include "storage.h"

#include <memory>
#include <sharksfin/api.h>
#include <sharksfin/Slice.h>
#include "transaction.h"
#include "iterator.h"

namespace jogasaki::kvs {

using sharksfin::Slice;
using sharksfin::StatusCode;

bool storage::delete_storage() {
    auto res = sharksfin::storage_delete(handle_);
    return res == sharksfin::StatusCode::OK;
}

bool storage::get(
    transaction& tx,
    std::string_view key,
    std::string_view& value
) {
    Slice v{};
    if(auto res = sharksfin::content_get(
            tx.handle(),
            handle_,
            key,
            &v); res != StatusCode::OK) {
        return false;
    }
    value = v.to_string_view();
    return true;
}

bool storage::put(
    transaction& tx,
    std::string_view key,
    std::string_view value
) {
    auto res = sharksfin::content_put(
        tx.handle(),
        handle_,
        key,
        value);
    return res == StatusCode::OK;
}

bool storage::remove(
    transaction& tx,
    std::string_view key
) {
    auto res = sharksfin::content_delete(
        tx.handle(),
        handle_,
        key);
    return res == StatusCode::OK;
}

bool storage::scan(transaction &tx,
    std::string_view begin_key, end_point_kind begin_kind,
    std::string_view end_key, end_point_kind end_kind,
    std::unique_ptr<iterator>& it
) {
    sharksfin::IteratorHandle handle{};
    if(auto res = sharksfin::content_scan(
            tx.handle(),
            handle_,
            sharksfin::Slice(begin_key), kind(begin_kind),
            sharksfin::Slice(end_key), kind(end_kind), &handle); res != sharksfin::StatusCode::OK) {
        return false;
    }
    it = std::make_unique<iterator>(handle);
    return true;
}

}

