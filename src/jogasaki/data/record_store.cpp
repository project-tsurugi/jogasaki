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
#include "record_store.h"

namespace jogasaki::data {

record_store::record_pointer record_store::append(accessor::record_ref record) {
    auto sz = meta_->record_size();
    auto* p = resource_->allocate(meta_->record_size(), meta_->record_alignment());
    if (!p) std::abort();
    copier_(p, sz, record);
    ++count_;
    return p;
}

std::size_t record_store::count() const noexcept {
    return count_;
}

bool record_store::empty() const noexcept {
    return count_ == 0;
}

void record_store::reset() noexcept {
    count_ = 0;
}

} // namespace
