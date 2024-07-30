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
#pragma once

#include <algorithm>
#include <cstddef>
#include <deque>
#include <boost/container/pmr/memory_resource.hpp>

#include <jogasaki/utils/interference_size.h>

#include "details/page_allocation_info.h"
#include "page_pool.h"
#include "paged_memory_resource.h"

namespace jogasaki::memory {

/**
 * @brief an implementation of paged_memory_resource that does not deallocate memory fragments.
 */
class cache_align monotonic_paged_memory_resource : public paged_memory_resource {
public:
    /**
     * @brief creates a new instance.
     * @param pool the parent page pool
     */
    explicit monotonic_paged_memory_resource(page_pool* pool)
        : page_pool_(pool)
    {}

    ~monotonic_paged_memory_resource() override;

    monotonic_paged_memory_resource(monotonic_paged_memory_resource const& other) = delete;
    monotonic_paged_memory_resource(monotonic_paged_memory_resource&& other) = delete;
    monotonic_paged_memory_resource& operator=(monotonic_paged_memory_resource const& other) = delete;
    monotonic_paged_memory_resource& operator=(monotonic_paged_memory_resource&& other) = delete;

    /**
     * @brief returned the number of holding pages.
     * @return the number of holding pages
     */
    [[nodiscard]] std::size_t count_pages() const noexcept;

    void end_current_page() override;

protected:
    /**
     * @brief allocates a new buffer.
     * @param bytes the required buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     * @return pointer to the allocated buffer
     * @throws std::bad_alloc if allocation was failed
     */
    [[nodiscard]] void* do_allocate(std::size_t bytes, std::size_t alignment) override;

    /**
     * @brief do nothing for monotonic memory resource.
     * @details this exists because do_deallocate() is pure virtual of memory_resource.
     * @param p pointer to the buffer to be deallocated
     * @param bytes the buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     */
    void do_deallocate(
        [[maybe_unused]] void* p,
        [[maybe_unused]] std::size_t bytes,
        [[maybe_unused]] std::size_t alignment
    ) override;

    [[nodiscard]] bool do_is_equal(const memory_resource& other) const noexcept override;

    [[nodiscard]] std::size_t do_page_remaining(std::size_t alignment) const noexcept override;

private:
    page_pool *page_pool_{};
    std::deque<details::page_allocation_info> pages_{};

    details::page_allocation_info& acquire_new_page();
};

} // namespace jogasaki::memory
