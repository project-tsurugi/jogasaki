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
#include "small_record_store.h"

#include <takatori/util/fail.h>

#include <jogasaki/utils/interference_size.h>
#include <jogasaki/utils/binary_printer.h>

namespace jogasaki::data {

using takatori::util::maybe_shared_ptr;
using takatori::util::fail;

small_record_store::small_record_store(maybe_shared_ptr<meta::record_meta> meta,
    memory::paged_memory_resource *varlen_resource, std::size_t capacity) :
    meta_(std::move(meta)),
    capacity_(capacity),
    varlen_resource_(varlen_resource),
    copier_(meta_, varlen_resource_),
    record_size_(meta_->record_size()),
    data_(utils::make_aligned_array<std::byte>(
        std::max(meta_->record_alignment(), utils::hardware_destructive_interference_size),
        record_size_*capacity_))
{}

small_record_store::record_pointer small_record_store::set(accessor::record_ref record, std::size_t index) {
    auto* p = ref(index).data();
    if (!p) fail();
    copier_(p, record_size_, record);
    return p;
}

std::size_t small_record_store::capacity() const noexcept {
    return capacity_;
}

small_record_store::operator bool() const noexcept {
    return static_cast<bool>(meta_);
}

accessor::record_ref small_record_store::ref(std::size_t index) const noexcept {
    return accessor::record_ref(data_.get()+record_size_*index, record_size_);
}

bool operator==(small_record_store const& a, small_record_store const& b) noexcept {
    return (!a && !b) || (a && b &&
        *a.meta_ == *b.meta_ &&
            a.capacity_ == b.capacity_ &&
            std::memcmp(a.data_.get(), b.data_.get(), a.record_size_*a.capacity_) == 0
    );
}

bool operator!=(small_record_store const& a, small_record_store const& b) noexcept {
    return !(a == b);
}

std::ostream &operator<<(std::ostream &out, const small_record_store &value) {
    if (value.meta_) {
        out << "meta: " << *value.meta_
            << " capacity: " << value.capacity_
            << " data: " << utils::binary_printer{value.data_.get(), value.capacity_*value.record_size_};
    } else {
        out << "<empty>";
    }
    return out;
}

} // namespace
