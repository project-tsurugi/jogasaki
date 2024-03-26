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
#include "page_allocation_info.h"

#include <jogasaki/memory/page_pool.h>

namespace jogasaki::memory::details {

std::size_t page_allocation_info::remaining(std::size_t alignment) const noexcept {
    auto head = reinterpret_cast<std::uintptr_t>(head_.address()); //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    auto last = head + page_size;
    auto ua_next = head + upper_bound_offset_;
    auto next = (ua_next + (alignment - 1)) / alignment * alignment;
    if (last < next) {
        return 0;
    }
    return last - next;
}

void *page_allocation_info::try_allocate_back(std::size_t bytes, std::size_t alignment) noexcept {
    // the next available block
    auto head = reinterpret_cast<std::uintptr_t>(head_.address()); //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    auto ua_next = head + upper_bound_offset_;
    auto next = (ua_next + (alignment - 1)) / alignment * alignment;
    auto next_lower_offset = next - head; // inclusive
    auto next_upper_offset = next_lower_offset + bytes; // exclusive

    if (next_upper_offset > page_size) {
        return nullptr;
    }
    // keep track the first alignment padding
    if (lower_bound_offset_ == upper_bound_offset_ && next_lower_offset > lower_bound_offset_) {
        lower_bound_offset_ = next_lower_offset;
    }
    upper_bound_offset_ = static_cast<offset_type>(next_upper_offset);
    return reinterpret_cast<void*>(next); //NOLINT
}

bool page_allocation_info::try_deallocate_front(void *p, std::size_t bytes, std::size_t alignment) {
    if (p < head_.address()) {
        return false;
    }
    auto head = reinterpret_cast<std::uintptr_t>(head_.address()); //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    auto start = reinterpret_cast<std::uintptr_t>(p) - head; //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    auto end = start + bytes;

    // LB <= start < LB + align
    // end <= UB
    if (start < lower_bound_offset_
        || start >= lower_bound_offset_ + alignment
        || end > upper_bound_offset_) {
        return false;
    }
    lower_bound_offset_ = static_cast<offset_type>(end);
    return true;
}

bool page_allocation_info::try_deallocate_back(void *p, std::size_t bytes, std::size_t) {
    if (p < head_.address()) {
        return false;
    }
    auto head = reinterpret_cast<std::uintptr_t>(head_.address()); //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    auto start = reinterpret_cast<std::uintptr_t>(p) - head; //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    auto end = start + bytes;

    // end <= UB
    if (end > upper_bound_offset_) {
        return false;
    }
    upper_bound_offset_ = static_cast<offset_type>(start);
    // remove the first alignment padding
    if (lower_bound_offset_ == upper_bound_offset_) {
        lower_bound_offset_ = 0;
        upper_bound_offset_ = 0;
    }
    return true;
}
} // namespace jogasaki::memory::details
