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
#include "lifo_paged_memory_resource.h"

#include <algorithm>
#include <cstdlib>
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

lifo_paged_memory_resource::~lifo_paged_memory_resource() {
    for (const auto& p : pages_) {
        release_deallocated_page(p.head());
    }
    if (reserved_page_) {
        page_pool_->release_page(reserved_page_);
        reserved_page_ = page_pool::page_info();
    }
}

std::size_t lifo_paged_memory_resource::count_pages() const noexcept {
    return pages_.size();
}

lifo_paged_memory_resource::checkpoint lifo_paged_memory_resource::get_checkpoint() const noexcept {
    if (pages_.empty()) {
        return initial_checkpoint;
    }
    auto& current = pages_.back();
    return { current.head().address(), current.upper_bound_offset() };
}

void lifo_paged_memory_resource::deallocate_after(lifo_paged_memory_resource::checkpoint const& point) {
    auto point_head = point.head_;
    if (point.head_ == nullptr && !pages_.empty()) {
        point_head = pages_.front().head().address();
                    // point.head_ is nullptr indicating that the checkpoint was taken when the pages_ was empty
    }
    while (!pages_.empty()) {
        auto&& page = pages_.back();
        if (page.head().address() == point_head) {
            // LB <= offset <= UB
            if (point.offset_ < page.lower_bound_offset()
                || point.offset_ > page.upper_bound_offset()) {
                std::abort();
            }
            page.upper_bound_offset(point.offset_);
            if (page.empty()) {
                release_deallocated_page(page.head());
                pages_.pop_back();
            }
            break;
        }
        release_deallocated_page(page.head());
        pages_.pop_back();
    }
}

void lifo_paged_memory_resource::end_current_page() {
    if (!pages_.empty()) {
        if (pages_.back().remaining(1) == page_size) {
            return;
        }
    }
    // allocate a new page
    acquire_new_page();
}

void *lifo_paged_memory_resource::do_allocate(std::size_t bytes, std::size_t alignment) {
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

void lifo_paged_memory_resource::do_deallocate(void *p, std::size_t bytes, std::size_t alignment) {
    if (pages_.empty()) {
        std::abort();
    }
    if(auto&& last = pages_.back(); last.empty()) {
        // release first if the current page is empty
        release_deallocated_page(last.head());
        pages_.pop_back();
    }
    auto&& last = pages_.back();
    if (!last.try_deallocate_back(p, bytes, alignment)) {
        std::abort();
    }
    // release if the resulting page is empty
    if (last.empty()) {
        release_deallocated_page(last.head());
        pages_.pop_back();
    }
}

bool lifo_paged_memory_resource::do_is_equal(boost::container::pmr::memory_resource const& other) const noexcept {
    return this == &other;
}

std::size_t lifo_paged_memory_resource::do_page_remaining(std::size_t alignment) const noexcept {
    if (pages_.empty()) {
        return 0;
    }
    return pages_.back().remaining(alignment);
}

details::page_allocation_info &lifo_paged_memory_resource::acquire_new_page() {
    page_pool::page_info new_page;
    if (reserved_page_) {
        new_page = reserved_page_;
        reserved_page_ = page_pool::page_info();
    } else {
        new_page = page_pool_->acquire_page();
        if (!new_page) {
            throw_exception(std::bad_alloc());
        }
    }
    return pages_.emplace_back(new_page);
}

void lifo_paged_memory_resource::release_deallocated_page(page_pool::page_info deallocated_page) {
    if (reserved_page_) {
        page_pool_->release_page(reserved_page_);
    }
    reserved_page_ = deallocated_page;
}

} // jogasaki::memory
