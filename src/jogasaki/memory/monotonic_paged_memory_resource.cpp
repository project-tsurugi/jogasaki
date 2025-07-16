/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "monotonic_paged_memory_resource.h"

#include <algorithm>
#include <memory>
#include <new>
#include <glog/logging.h>

#include <takatori/util/exception.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/memory/details/page_allocation_info.h>
#include <jogasaki/memory/page_pool.h>

namespace jogasaki::memory {

using takatori::util::throw_exception;

monotonic_paged_memory_resource::~monotonic_paged_memory_resource() {
    for (const auto& p : pages_) {
        page_pool_->release_page(p.head());
    }
}

std::size_t monotonic_paged_memory_resource::count_pages() const noexcept {
    return pages_.size();
}

void monotonic_paged_memory_resource::end_current_page() {
    if (!pages_.empty()) {
        if (pages_.back().remaining(1) == page_size) {
            return;
        }
    }
    // allocate a new page
    acquire_new_page();
}

void *monotonic_paged_memory_resource::do_allocate(std::size_t bytes, std::size_t alignment) {
    // try acquire in the current page
    if (!pages_.empty()) {
        auto&& current = pages_.back();
        if (auto* ptr = current.try_allocate_back(bytes, alignment); ptr != nullptr) {
            return ptr;
        }
    }

    // then use a new page
    auto&& current = acquire_new_page();
    if (auto* ptr = current.try_allocate_back(bytes, alignment); ptr != nullptr) {
        return ptr;
    }

    LOG_LP(ERROR) << "invalid memory request bytes:" << bytes << " alignment:" << alignment;
    throw_exception(std::bad_alloc());
}

void monotonic_paged_memory_resource::do_deallocate(void *p, std::size_t bytes, std::size_t alignment) {
    // do nothing
    (void) p;
    (void) bytes;
    (void) alignment;
}

bool monotonic_paged_memory_resource::do_is_equal(const boost::container::pmr::memory_resource &other) const noexcept {
    return this == &other;
}

std::size_t monotonic_paged_memory_resource::do_page_remaining(std::size_t alignment) const noexcept {
    if (pages_.empty()) {
        return 0;
    }
    return pages_.back().remaining(alignment);
}

details::page_allocation_info &monotonic_paged_memory_resource::acquire_new_page() {
    page_pool::page_info new_page = page_pool_->acquire_page();
    if (!new_page) {
        throw_exception(std::bad_alloc());
    }
    return pages_.emplace_back(new_page);
}
} // namespace jogasaki::memory
