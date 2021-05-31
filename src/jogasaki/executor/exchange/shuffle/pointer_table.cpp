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
#include "pointer_table.h"

#include <array>
#include <vector>
#include <cstring>

#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/accessor/record_copier.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::exchange::shuffle {

pointer_table::pointer_table(
    memory::paged_memory_resource *resource,
    std::size_t capacity
) :
    resource_(resource),
    capacity_(capacity)
{
    head_ = static_cast<iterator>(resource_->allocate(sizeof(pointer)*capacity_, alignof(pointer)));
}

pointer_table::pointer pointer_table::emplace_back(pointer_table::pointer p) {
    if (size_ < capacity_) {
        head_[size_] = p;
        ++size_;
    }
    return p;
}

std::size_t pointer_table::size() const noexcept {
    return size_;
}

std::size_t pointer_table::capacity() const noexcept {
    return capacity_;
}

bool pointer_table::empty() const noexcept {
    return size_ == 0;
}

pointer_table::iterator pointer_table::begin() const noexcept {
    return head_;
}

pointer_table::iterator pointer_table::end() const noexcept {
    return head_+size_;
}

} // namespace
