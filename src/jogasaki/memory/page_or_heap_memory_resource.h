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
#pragma once

#include <cstddef>
#include <boost/container/pmr/memory_resource.hpp>

#include <jogasaki/utils/interference_size.h>

#include "monotonic_paged_memory_resource.h"
#include "page_pool.h"

namespace jogasaki::memory {

/**
 * @brief an implementation of memory_resource that serves allocations fitting in a page from a
 * monotonic pooled page and spills larger ones to the heap.
 * @details a page-backed resource cannot serve a single allocation larger than one page. This
 * resource keeps the page pool benefit for the allocations that do fit, while still being able to
 * serve larger contiguous blocks, which it takes from the process heap via new_delete_resource.
 *
 * This is intended for containers whose storage is a single contiguous block that is reallocated
 * (relocated) as it grows, such as the hash table used by count_distinct: the small tables stay on
 * a pooled page and only the grown ones reach the heap, where the superseded block is reclaimed.
 *
 * @note contrary to monotonic_paged_memory_resource, this resource does not guarantee that all allocations are
 * reclaimed on destruction. Allocations served from the heap are reclaimed on deallocate, so the caller must ensure to
 * call deallocate to avoid leaks - unlike a purely monotonic resource, this object does not track them.
 * @note this class is not thread-safe.
 */
class cache_align page_or_heap_memory_resource : public boost::container::pmr::memory_resource {
public:
    /**
     * @brief creates a new instance.
     * @param pool the page pool that backs the allocations fitting in a page
     */
    explicit page_or_heap_memory_resource(page_pool* pool) : paged_(pool) {}

    ~page_or_heap_memory_resource() override = default;

    page_or_heap_memory_resource(page_or_heap_memory_resource const& other) = delete;
    page_or_heap_memory_resource(page_or_heap_memory_resource&& other) = delete;
    page_or_heap_memory_resource& operator=(page_or_heap_memory_resource const& other) = delete;
    page_or_heap_memory_resource& operator=(page_or_heap_memory_resource&& other) = delete;

    /**
     * @brief returns the number of pages acquired from the pool so far.
     * @return the number of pages held by the backing paged resource
     */
    [[nodiscard]] std::size_t count_pages() const noexcept;

protected:
    /**
     * @brief allocates a new buffer.
     * @details the allocation is served from the page pool if it fits in a page, otherwise from the
     * heap.
     * @param bytes the required buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     * @return pointer to the allocated buffer
     * @throws std::bad_alloc if allocation was failed
     */
    [[nodiscard]] void* do_allocate(std::size_t bytes, std::size_t alignment) override;

    /**
     * @brief deallocates the buffer allocated by this resource.
     * @param p pointer to the buffer to be deallocated
     * @param bytes the buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     */
    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;

    [[nodiscard]] bool do_is_equal(memory_resource const& other) const noexcept override;

private:
    monotonic_paged_memory_resource paged_;
};

} // namespace jogasaki::memory
