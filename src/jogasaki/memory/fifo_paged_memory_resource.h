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
#pragma once

#include <algorithm>
#include <deque>

#include <jogasaki/utils/interference_size.h>
#include "paged_memory_resource.h"
#include "page_pool.h"
#include "details/page_allocation_info.h"

namespace jogasaki::memory {

/**
 * @brief an implementation of paged_memory_resource that only can deallocate memory fragments by FIFO order.
 */
class cache_align fifo_paged_memory_resource : public paged_memory_resource {
public:
    /**
     * @brief creates a new instance.
     * @param pool the parent page pool
     */
    explicit fifo_paged_memory_resource(page_pool* pool)
        : page_pool_(pool)
    {}

    ~fifo_paged_memory_resource() override;

    fifo_paged_memory_resource(fifo_paged_memory_resource const& other) = delete;
    fifo_paged_memory_resource(fifo_paged_memory_resource&& other) = delete;
    fifo_paged_memory_resource& operator=(fifo_paged_memory_resource const& other) = delete;
    fifo_paged_memory_resource& operator=(fifo_paged_memory_resource&& other) = delete;

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
     * @brief returns a checkpoint of the current allocated region.
     * @return the checkpoint
     */
    [[nodiscard]] checkpoint get_checkpoint() const noexcept;

    /**
     * @brief releases the allocated region before the given checkpoint.
     * @param point the checkpoint
     */
    void deallocate_before(checkpoint const& point);

    void end_current_page() noexcept override;

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
     * @details this only can deallocate by FIFO order, that is, this function only accepts the eldest
     *      memory region which has not yet been deallocated.
     * @param p pointer to the buffer to be deallocated
     * @param bytes the buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     * @warning undefined behavior if the given memory fragment is not the eldest one
     */
    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override;

    [[nodiscard]] bool do_is_equal(const memory_resource& other) const noexcept override;

    [[nodiscard]] std::size_t do_page_remaining(std::size_t alignment) const noexcept override;

private:
    page_pool *page_pool_{};
    std::deque<details::page_allocation_info> pages_{};

    details::page_allocation_info& acquire_new_page();
};

} // namespace jogasaki::memory
