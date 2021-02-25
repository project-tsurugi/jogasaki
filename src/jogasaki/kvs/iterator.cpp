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
#include "iterator.h"

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

bool iterator::key(std::string_view& k) const {
    sharksfin::Slice slice{};
    if(sharksfin::StatusCode res = sharksfin::iterator_get_key(handle_, &slice);res != sharksfin::StatusCode::OK) {
        return false;
    }
    k = slice.to_string_view();
    return true;
}

bool iterator::value(std::string_view& v) const {
    sharksfin::Slice slice{};
    if(sharksfin::StatusCode res = sharksfin::iterator_get_value(handle_, &slice);res != sharksfin::StatusCode::OK) {
        return false;
    }
    v = slice.to_string_view();
    return true;
}

sharksfin::IteratorHandle iterator::handle() const noexcept {
    return handle_;
}

}

