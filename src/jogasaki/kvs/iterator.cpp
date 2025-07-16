/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "iterator.h"

#include <sharksfin/Slice.h>
#include <sharksfin/StatusCode.h>
#include <sharksfin/api.h>

#include <jogasaki/kvs/error.h>

namespace jogasaki::kvs {

iterator::iterator(sharksfin::IteratorHandle handle) : handle_(handle) {}

iterator::~iterator() noexcept {
    sharksfin::iterator_dispose(handle_);
}

status iterator::next() {
    sharksfin::StatusCode res = sharksfin::iterator_next(handle_);
    if (res == sharksfin::StatusCode::OK) {
        return status::ok;
    }
    if (res == sharksfin::StatusCode::NOT_FOUND) {
        return status::not_found;
    }
    return resolve(res);
}

status iterator::read_key(std::string_view& k) const {
    sharksfin::Slice slice{};
    auto res = sharksfin::iterator_get_key(handle_, &slice);
    if(res == sharksfin::StatusCode::OK) {
        k = slice.to_string_view();
    }
    return resolve(res);
}

status iterator::read_value(std::string_view& v) const {
    sharksfin::Slice slice{};
    auto res = sharksfin::iterator_get_value(handle_, &slice);
    if(res == sharksfin::StatusCode::OK) {
        v = slice.to_string_view();
    }
    return resolve(res);
}

sharksfin::IteratorHandle iterator::handle() const noexcept {
    return handle_;
}

}

