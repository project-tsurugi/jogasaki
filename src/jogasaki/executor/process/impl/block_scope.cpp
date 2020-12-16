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
#include "block_scope.h"

namespace jogasaki::executor::process::impl {

block_scope::block_scope(const block_scope_info& info) :
    info_(std::addressof(info)),
    store_(std::make_unique<data::small_record_store>(info.meta()))
{}

data::small_record_store& block_scope::store() const noexcept {
    return *store_;
}

variable_value_map const& block_scope::value_map() const noexcept {
    return info_->value_map();
}

const maybe_shared_ptr<meta::record_meta>& block_scope::meta() const noexcept {
    return info_->meta();
}
}


