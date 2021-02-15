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

small_record_store::small_record_store(
    maybe_shared_ptr<meta::record_meta> meta,
    memory::paged_memory_resource *varlen_resource
) :
    meta_(std::move(meta)),
    varlen_resource_(varlen_resource),
    copier_(meta_, varlen_resource_),
    record_size_(meta_->record_size()),
    buf_(
        record_size_,
        std::max(meta_->record_alignment(), utils::hardware_destructive_interference_size)
    )
{}

small_record_store::record_pointer small_record_store::set(accessor::record_ref record) {
    auto* p = ref().data();
    if (!p) fail();
    copier_(p, record_size_, record);
    return p;
}

small_record_store::operator bool() const noexcept {
    return static_cast<bool>(meta_);
}

accessor::record_ref small_record_store::ref() const noexcept {
    return accessor::record_ref(buf_.data(), record_size_);
}

bool operator==(small_record_store const& a, small_record_store const& b) noexcept {
    return (!a && !b) || (a && b &&
        *a.meta_ == *b.meta_ &&
            std::memcmp(a.buf_.data(), b.buf_.data(), a.record_size_) == 0
    );
}

bool operator!=(small_record_store const& a, small_record_store const& b) noexcept {
    return !(a == b);
}

std::ostream &operator<<(std::ostream &out, const small_record_store &value) {
    if (value.meta_) {
        out << "meta: " << *value.meta_
            << " data: " << utils::binary_printer{value.buf_.data(), value.record_size_};
    } else {
        out << "<empty>";
    }
    return out;
}

} // namespace
