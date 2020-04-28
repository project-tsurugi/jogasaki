#pragma once

#include <limits>
#include <deque>

#include <cstdint>

#include "../page_pool.h"

namespace dc::memory::details {

/**
 * @brief the allocated page information.
 */
class page_allocation_info {
public:
    /**
     * @brief creates a new instance.
     * @param ptr the page pointer
     */
    explicit constexpr page_allocation_info(void* ptr) noexcept : head_(ptr) {}

    /**
     * @brief returns pointer to the allocated page.
     * @return the page pointer
     */
    [[nodiscard]] constexpr void* head() const noexcept { return head_; }

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
    [[nodiscard]] std::size_t remaining(std::size_t alignment) const noexcept {
        auto head = reinterpret_cast<std::uintptr_t>(head_);
        auto last = head + page_size;
        auto ua_next = head + upper_bound_offset_;
        auto next = (ua_next + (alignment - 1)) / alignment * alignment;
        if (last < next) {
            return 0;
        }
        return last - next;
    }

    /**
     * @brief allocates the next region.
     * @param bytes the region size
     * @param alignment the starting alignment size
     * @return the allocated region
     * @return nullptr if there is no remaining region to allocate
     */
    void* try_allocate_back(std::size_t bytes, std::size_t alignment) noexcept {
        // the next available block
        auto head = reinterpret_cast<std::uintptr_t>(head_);
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
        return reinterpret_cast<void*>(next);
    }

    /**
     * @brief deallocates the allocated region from the head of the page.
     * @param p the allocated pointer
     * @param bytes the allocated size
     * @return true if successfully deallocated
     * @return false otherwise
     */
    bool try_deallocate_front(void* p, std::size_t bytes, std::size_t alignment) {
        if (p < head_) {
            return false;
        }
        auto head = reinterpret_cast<std::uintptr_t>(head_);
        auto start = reinterpret_cast<std::uintptr_t>(p) - head;
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

    /**
     * @brief deallocates the allocated region from the tail of the page.
     * @param p the allocated pointer
     * @param bytes the allocated size
     * @return true if successfully deallocated
     * @return false otherwise
     */
    bool try_deallocate_back(void* p, std::size_t bytes, std::size_t) {
        if (p < head_) {
            return false;
        }
        auto head = reinterpret_cast<std::uintptr_t>(head_);
        auto start = reinterpret_cast<std::uintptr_t>(p) - head;
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

private:
    // the page head
    void* head_;

    using offset_type = std::uint32_t;
    static_assert(page_size <= std::numeric_limits<offset_type>::max());

    // the lower offset of allocated region (inclusive)
    offset_type lower_bound_offset_ {};

    // the upper offset of allocated region (exclusive)
    offset_type upper_bound_offset_ {};
};

} // dc::memory
