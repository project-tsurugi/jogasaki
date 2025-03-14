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
#include "result_set.h"

#include <optional>
#include <string>
#include <utility>
#include <vector>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/impl/result_set_iterator.h>
#include <jogasaki/data/result_store.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/utils/make_shared_cache_aligned.h>

namespace jogasaki::api::impl {

result_set::result_set(std::unique_ptr<data::result_store> store) noexcept:
    store_(std::move(store)),
    meta_(utils::make_shared_cache_aligned<meta::external_record_meta>(
        store_->meta() ?
        store_->meta() :
        utils::make_shared_cache_aligned<meta::record_meta>(), // it's possible store has no result and no metadata
        std::vector<std::optional<std::string>>{}
    ))
{}

api::record_meta const* result_set::meta() const noexcept {
    return std::addressof(meta_);
}

std::unique_ptr<api::result_set_iterator> result_set::iterator() const {
    return std::make_unique<impl::result_set_iterator>(
        store_->begin(),
        store_->end(),
        store_->meta());
}

void result_set::close() {
    store_.reset();
}
}
