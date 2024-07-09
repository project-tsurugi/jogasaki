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
#include "record_store.h"

#include <cstdlib>
#include <type_traits>
#include <utility>

#include <jogasaki/accessor/record_copier.h>
#include <jogasaki/memory/paged_memory_resource.h>

namespace jogasaki::data {

record_store::record_pointer record_store::append(accessor::record_ref record) {
    auto* p = resource_->allocate(positive_record_size_, meta_->record_alignment());
    if (!p) std::abort();
    copier_(p, original_record_size_, record);
    ++count_;
    return p;
}

record_store::record_pointer record_store::allocate_record() {
    auto* p = resource_->allocate(positive_record_size_, meta_->record_alignment());
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
    original_record_size_(meta_->record_size()),
    positive_record_size_(original_record_size_ == 0 ? 1 : original_record_size_)
{
    // if record size is 0, the alignment must be 1
    BOOST_ASSERT(original_record_size_ != 0 || meta_->record_alignment() == 1);  //NOLINT
}

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
