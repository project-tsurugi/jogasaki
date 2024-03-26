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
#include "result_set_iterator.h"

#include <utility>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/record.h>
#include <jogasaki/data/result_store.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;

result_set_iterator::result_set_iterator(
    data::result_store::iterator it,
    data::result_store::iterator end,
    maybe_shared_ptr<meta::record_meta> const& meta
) :
    it_(it),
    end_(end),
    record_(meta)
{}

bool result_set_iterator::has_next() const noexcept {
    return it_ != end_;
}

record* result_set_iterator::next() {
    if (it_ == end_) {
        return {};
    }
    record_.ref(*it_);
    ++it_;
    return std::addressof(record_);
}

}
