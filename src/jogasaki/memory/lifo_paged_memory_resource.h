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
 * @brief an implementation of paged_memory_resource that only can deallocate memory fragments by LIFO order.
 */
class cache_align lifo_paged_memory_resource : public paged_memory_resource {
public:
    /**
     * @brief creates a new instance.
     * @param pool the parent page pool
     */
    explicit lifo_paged_memory_resource(page_pool* pool)
        : page_pool_(pool)
    {}

    ~lifo_paged_memory_resource() override;

    lifo_paged_memory_resource(lifo_paged_memory_resource const& other) = delete;
    lifo_paged_memory_resource(lifo_paged_memory_resource&& other) = delete;
    lifo_paged_memory_resource& operator=(lifo_paged_memory_resource const& other) = delete;
    lifo_paged_memory_resource& operator=(lifo_paged_memory_resource&& other) = delete;

    /**
     * @brief returned the number of holding pages.
     * @return the number of holding pages
     */
    [[nodiscard]] std::size_t count_pages() const noexcept;

    /**
     * @brief the checkpoint of allocated region.
     */
    struct checkpoint {
        /// @private
        void* head_;
        /// @private
        std::size_t offset_;
    };

    /**
     * @brief checkpoint constant that indicates the very beginning
     * @details this can be used to clear all allocated resource
     */
    constexpr static checkpoint initial_checkpoint{nullptr, 0};

    /**
     * @brief returns a checkpoint of the current allocated region.
     * @return the checkpoint
     */
    [[nodiscard]] checkpoint get_checkpoint() const noexcept;

    /**
     * @brief releases the allocated region before the given checkpoint.
     * @param point the checkpoint
     */
    void deallocate_after(checkpoint const& point);

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
     * @brief deallocates the buffer previously allocated by this resource.
     * @details this only can deallocate by LIFO order, that is, this function only accepts the youngest
     *      memory region which has not yet been deallocated.
     * @param p pointer to the buffer to be deallocated
     * @param bytes the buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     * @warning undefined behavior if the given memory fragment is not the youngest one
     */
    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;

    [[nodiscard]] bool do_is_equal(const memory_resource& other) const noexcept override;

    [[nodiscard]] std::size_t do_page_remaining(std::size_t alignment) const noexcept override;

private:
    page_pool *page_pool_{};
    std::deque<details::page_allocation_info> pages_{};
    page_pool::page_info reserved_page_{};

    details::page_allocation_info& acquire_new_page();
    void release_deallocated_page(page_pool::page_info);
};

/**
 * @brief equality comparison operator
 */
inline bool operator==(
    lifo_paged_memory_resource::checkpoint const& a,
    lifo_paged_memory_resource::checkpoint const& b
) noexcept {
    return a.head_ == b.head_ &&
        a.offset_ == b.offset_;
}

/**
 * @brief inequality comparison operator
 */
inline bool operator!=(
    lifo_paged_memory_resource::checkpoint const& a,
    lifo_paged_memory_resource::checkpoint const& b
) noexcept {
    return !(a == b);
}

} // jogasaki::memory
