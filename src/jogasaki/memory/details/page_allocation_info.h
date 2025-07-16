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
#include <cstdint>
#include <deque>
#include <limits>

#include "../page_pool.h"

namespace jogasaki::memory::details {

/**
 * @brief the allocated page information.
 */
class page_allocation_info {
public:
    /**
     * @brief creates a new instance.
     * @param ptr the page pointer
     */
    explicit constexpr page_allocation_info(page_pool::page_info ptr) noexcept : head_(ptr) {}

    /**
     * @brief returns pointer to the allocated page.
     * @return the page pointer
     */
    [[nodiscard]] constexpr page_pool::page_info head() const noexcept { return head_; }

    /**
     * @brief returns whether or not this page is empty.
     * @return true if this page has no allocated area
     * @return false otherwise
     */
    [[nodiscard]] constexpr bool empty() const noexcept { return upper_bound_offset_ == lower_bound_offset_; }

    /**
     * @brief returns the lower bound offset of allocated region in page.
     * @return the lower bound offset (inclusive)
     */
    [[nodiscard]] constexpr std::size_t lower_bound_offset() const noexcept {
        return lower_bound_offset_;
    }

    /**
     * @brief sets the lower bound offset of the allocated region in page.
     * @param offset the offset (inclusive)
     */
    constexpr void lower_bound_offset(std::size_t offset) noexcept {
        lower_bound_offset_ = static_cast<offset_type>(offset);
    }

    /**
     * @brief returns the upper bound offset of allocated region in page.
     * @return the upper bound offset (exclusive)
     */
    [[nodiscard]] constexpr std::size_t upper_bound_offset() const noexcept {
        return upper_bound_offset_;
    }

    /**
     * @brief sets the upper bound offset of the allocated region in page.
     * @param offset the offset (exclusive)
     */
    constexpr void upper_bound_offset(std::size_t offset) noexcept {
        upper_bound_offset_ = static_cast<offset_type>(offset);
    }

    /**
     * @brief returns the remaining bytes in this page.
     * @param alignment the starting alignment size
     * @return the remaining size
     */
    [[nodiscard]] std::size_t remaining(std::size_t alignment) const noexcept;

    /**
     * @brief allocates the next region.
     * @param bytes the region size
     * @param alignment the starting alignment size
     * @return the allocated region
     * @return nullptr if there is no remaining region to allocate
     */
    void* try_allocate_back(std::size_t bytes, std::size_t alignment) noexcept;

    /**
     * @brief deallocates the allocated region from the head of the page.
     * @param p the allocated pointer
     * @param bytes the allocated size
     * @return true if successfully deallocated
     * @return false otherwise
     */
    bool try_deallocate_front(void* p, std::size_t bytes, std::size_t alignment);

    /**
     * @brief deallocates the allocated region from the tail of the page.
     * @param p the allocated pointer
     * @param bytes the allocated size
     * @return true if successfully deallocated
     * @return false otherwise
     */
    bool try_deallocate_back(void* p, std::size_t bytes, std::size_t);

private:
    // the page head
    page_pool::page_info head_;

    using offset_type = std::uint32_t;
    static_assert(page_size <= std::numeric_limits<offset_type>::max());

    // the lower offset of allocated region (inclusive)
    offset_type lower_bound_offset_ {};

    // the upper offset of allocated region (exclusive)
    offset_type upper_bound_offset_ {};
};

} // namespace jogasaki::memory::details
