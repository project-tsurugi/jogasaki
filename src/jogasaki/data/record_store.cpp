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
    auto* p = resource_->allocate(record_size_, meta_->record_alignment());
    if (!p) std::abort();
    copier_(p, record_size_, record);
    ++count_;
    return p;
}

record_store::record_pointer record_store::allocate_record() {
    auto* p = resource_->allocate(record_size_, meta_->record_alignment());
    if (!p) std::abort();
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

record_store::record_store(
    memory::paged_memory_resource* record_resource,
    memory::paged_memory_resource* varlen_resource,
    maybe_shared_ptr<meta::record_meta> meta
) :
    resource_(record_resource),
    varlen_resource_(varlen_resource),
    meta_(std::move(meta)),
    copier_(meta_, varlen_resource),
    record_size_(meta_->record_size())
{}

maybe_shared_ptr<meta::record_meta> const& record_store::meta() const noexcept {
    return meta_;
}

memory::paged_memory_resource* record_store::varlen_resource() const noexcept {
    return varlen_resource_;
}

accessor::record_copier& record_store::copier() noexcept {
    return copier_;
}

} // namespace
