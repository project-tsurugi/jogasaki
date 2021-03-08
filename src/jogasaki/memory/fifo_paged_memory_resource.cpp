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
#include "fifo_paged_memory_resource.h"

namespace jogasaki::memory {

fifo_paged_memory_resource::~fifo_paged_memory_resource() {
    for (const auto& p : pages_) {
        page_pool_->release_page(p.head());
    }
}

std::size_t fifo_paged_memory_resource::count_pages() const noexcept {
    return pages_.size();
}

fifo_paged_memory_resource::checkpoint fifo_paged_memory_resource::get_checkpoint() const noexcept {
    if (pages_.empty()) {
        return { nullptr, 0 };
    }
    auto& current = pages_.back();
    return { current.head().address(), current.upper_bound_offset() };
}

void fifo_paged_memory_resource::deallocate_before(fifo_paged_memory_resource::checkpoint const& point) {
    if (point.head_ == nullptr) {
        return;
    }
    while (!pages_.empty()) {
        auto&& page = pages_.front();
        if (page.head().address() == point.head_) {
            // LB <= offset <= UB
            if (point.offset_ < page.lower_bound_offset()
                || point.offset_ > page.upper_bound_offset()) {
                std::abort();
            }
            page.lower_bound_offset(point.offset_);
            if (page.empty()) {
                page_pool_->release_page(page.head());
                pages_.pop_front();
            }
            break;
        }
        page_pool_->release_page(page.head());
        pages_.pop_front();
    }
}

void fifo_paged_memory_resource::end_current_page() noexcept {
    if (!pages_.empty()) {
        if (pages_.back().remaining(1) == page_size) {
            return;
        }
    }
    // allocate a new page
    acquire_new_page();
}

void *fifo_paged_memory_resource::do_allocate(std::size_t bytes, std::size_t alignment) {
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

    throw std::bad_alloc();
}

void fifo_paged_memory_resource::do_deallocate(void *p, std::size_t bytes, std::size_t alignment) {
    if (pages_.empty()) {
        std::abort();
    }

    auto&& last = pages_.front();
    if (!last.try_deallocate_front(p, bytes, alignment)) {
        std::abort();
    }

    // release if the page is empty
    if (last.empty()) {
        page_pool_->release_page(last.head());
        pages_.pop_front();
    }
}

bool fifo_paged_memory_resource::do_is_equal(boost::container::pmr::memory_resource const& other) const noexcept {
    return this == &other;
}

std::size_t fifo_paged_memory_resource::do_page_remaining(std::size_t alignment) const noexcept {
    if (pages_.empty()) {
        return 0;
    }
    return pages_.back().remaining(alignment);
}

details::page_allocation_info &fifo_paged_memory_resource::acquire_new_page() {
    page_pool::page_info new_page = page_pool_->acquire_page();
    if (!new_page) {
        throw std::bad_alloc();
    }
    return pages_.emplace_back(new_page);
}

} // namespace jogasaki::memory
