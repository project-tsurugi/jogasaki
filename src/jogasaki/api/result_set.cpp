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
#include <jogasaki/api/result_set.h>

namespace jogasaki::api {

class result_set::iterator {

};

class result_set::impl {
public:
    impl() = default;

    iterator begin();
    iterator end();

    void close();
};

result_set::iterator result_set::impl::begin() {
    return {};
}

result_set::iterator result_set::impl::end() {
    return {};
}

void result_set::impl::close() {

};

result_set::result_set() : impl_(std::make_unique<result_set::impl>()) {}
result_set::~result_set() = default;

result_set::iterator result_set::begin() {
    return impl_->begin();
}

result_set::iterator result_set::end() {
    return impl_->begin();
}

void result_set::close() {
    impl_->close();
}

}
