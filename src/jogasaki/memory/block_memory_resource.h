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

#include <bitset>
#include <map>
#include <stdexcept>
#include <utility>
#include <cassert>

#include <jogasaki/utils/interference_size.h>
#include "page_pool.h"
#include "paged_memory_resource.h"

namespace jogasaki::memory {

/**
 * @brief an implementation of paged_memory_resource that divides pages into fixed size small blocks.
 * @details In contrast to the other implementations, this can release any allocated memory fragments.
 *      This always expends memory region of multiple number of the block size even if the requested size is
 *      smaller than the block, that is, this may not suitable to allocate the large number of small regions.
 * @tparam BlockSize the block size in bytes.
 *      It must satisfy the following all constraints:
 *
 *      * block size must be >= sizeof(void*) * 2
 *      * block size must be <= page_size
 *      * block size must be a factor of page_size
 *      * alignment of each allocation must be <= block_size / 2
 *
 *      Note that, the block size should be multiple number of page_pool::min_alignment,
 *      and the recommended value is `65,536` (32 blocks per page).
 */
template<std::size_t BlockSize>
class cache_align block_memory_resource : public paged_memory_resource {
public:
    /// @brief the block size in bytes.
    static inline constexpr std::size_t block_size = BlockSize;

    /// @brief the number of blocks in each page.
    static inline constexpr std::size_t nblocks_in_page = page_size / block_size;

    static_assert(block_size >= sizeof(void*) * 2);
    static_assert(block_size <= page_size);
    static_assert(page_size % block_size == 0);

    /**
     * @brief creates a new instance.
     * @param pool the parent page pool
     * @param reuse_page whether or not this re-uses de-allocated page instead of release it
     */
    explicit block_memory_resource(page_pool& pool, bool reuse_page = true) noexcept
        : page_pool_(std::addressof(pool))
        , reuse_page_(reuse_page)
    {}

    ~block_memory_resource() override {
        for (auto iter = blocks_.begin(); iter != blocks_.end(); ++iter) {
            page_pool_->release_page(iter->first);
        }
    }

    block_memory_resource(block_memory_resource const&) = delete;
    block_memory_resource& operator=(block_memory_resource const&) = delete;
    block_memory_resource(block_memory_resource&& other) noexcept = default;
    block_memory_resource& operator=(block_memory_resource&& other) noexcept = default;

    void end_current_page() override {
        active_ = nullptr;
    }

    /**
     * @brief returned the number of holding pages.
     * @return the number of holding pages
     */
    [[nodiscard]] std::size_t count_pages() const noexcept {
        return blocks_.size();
    }

protected:
    /**
     * @brief allocates a new buffer.
     * @param bytes the required buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     * @return pointer to the allocated buffer
     * @throws std::bad_alloc if allocation was failed
     */
    [[nodiscard]] void* do_allocate(std::size_t bytes, std::size_t alignment) override {
        if (alignment > block_size / 2) {
            throw std::bad_alloc();
        }

        // try acquire blocks from the current page only if it exists
        if (active_ != nullptr) {
            if (auto* r = active_->try_acquire(bytes, alignment); r != nullptr) {
                return r;
            }
        }

        // acquire a new page
        auto next_head = page_pool_->acquire_page();
        auto [iter, success] = blocks_.emplace(next_head, block_info { next_head });
        if (!success) {
            throw std::bad_alloc();
        }
        auto&& next = iter->second;

        // acquire blocks from the new page
        if (auto* r = next.try_acquire(bytes, alignment); r != nullptr) {
            // sets the new page as active
            if (active_ == nullptr
                    || next.remaining_blocks() >= active_->remaining_blocks()) {
                active_ = std::addressof(next);
            }
            return r;
        }

        throw std::bad_alloc();
    }

    /**
     * @brief deallocates the buffer previously allocated by this resource.
     * @param p pointer to the buffer to be deallocated
     * @param bytes the buffer size in bytes
     */
    void do_deallocate(void* p, std::size_t bytes, std::size_t) override {
        assert(reinterpret_cast<std::uintptr_t>(p) >= page_size); // NOLINT

        // find a page which contains this block
        page_pool::page_info prev(
            static_cast<char*>(p) - page_size,  //NOLINT
            page_pool::page_info::undefined_numa_node
        );
        auto iter = blocks_.upper_bound(prev);
        if (iter == blocks_.end()) {
            // no such page
            return;
        }

        // releases the blocks in the found page
        auto&& block = iter->second;
        auto offset = static_cast<std::size_t>(static_cast<char*>(p) - static_cast<char*>(block.head().address()));
        if (offset >= page_size) {
            // no such page
            return;
        }

        block.release(offset, bytes, reuse_page_);
        if (block.empty()) {
            // try re-use the empty page as the current active page
            if (reuse_page_) {
                // continue to use this page
                if (active_ == std::addressof(block)) {
                    return;
                }
                // use the released page as the current active page
                if (active_ == nullptr || !active_->empty()) {
                    active_ = std::addressof(block);
                    return;
                }
            }
            // otherwise, gives back the acquired page
            page_pool_->release_page(block.head());
            if (std::addressof(block) == active_) {
                active_ = nullptr;
            }
            blocks_.erase(block.head());
        }
    }

    /**
     * @brief returns whether or not the given memory resource is equivalent to this.
     * @param other the target memory resource
     * @return true if the both are equivalent
     * @return false otherwise
     */
    [[nodiscard]] bool do_is_equal(memory_resource const& other) const noexcept override {
        return this == std::addressof(other);
    }

    [[nodiscard]] std::size_t do_page_remaining(std::size_t alignment) const noexcept override {
        if (active_ != nullptr) {
            return active_->remaining(alignment);
        }
        return unknown_size;
    }

private:
    class block_info {
    public:
        explicit constexpr block_info(page_pool::page_info head) noexcept : head_(head) {}
        [[nodiscard]] constexpr page_pool::page_info head() const noexcept { return head_; }
        [[nodiscard]] constexpr bool empty() const noexcept { return released_.count() == acquired_; }
        [[nodiscard]] constexpr std::size_t remaining_blocks() const noexcept { return nblocks_in_page - acquired_; }
        [[nodiscard]] std::size_t remaining(std::size_t alignment) const noexcept {
            auto head = reinterpret_cast<std::uintptr_t>(head_.address()); //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            auto last = head + page_size;
            auto ua_next = head + acquired_ * block_size;
            auto next = (ua_next + (alignment - 1)) / alignment * alignment;
            if (last < next) {
                return unknown_size;
            }
            return last - next;
        }
        [[nodiscard]] void* try_acquire(std::size_t bytes, std::size_t alignment) noexcept {
            // the next available block
            auto head = reinterpret_cast<std::uintptr_t>(head_.address()); //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            auto ua_next = head + acquired_ * block_size;
            auto next = (ua_next + (alignment - 1)) / alignment * alignment;
            assert(next >= ua_next); // NOLINT
            assert(next - ua_next < block_size); // NOLINT

            auto start_block = (next - head) / block_size; // inclusive
            auto last_block = (next + bytes - head + block_size - 1) / block_size; // exclusive
            auto nblocks = last_block - start_block;

            if (acquired_ + nblocks > nblocks_in_page) {
                return nullptr;
            }
            acquired_ += nblocks;
            return reinterpret_cast<void*>(next); //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        }
        void release(std::size_t offset, std::size_t bytes, bool compaction) noexcept {
            auto start_block = offset / block_size; // inclusive
            auto last_block = (offset + bytes + block_size - 1) / block_size; // exclusive
            auto nblocks = last_block - start_block;
            for (std::size_t i = 0; i < nblocks; ++i) {
                auto b = start_block + i;
                assert(acquired_ > b);
                assert(!released_.test(b)); // NOLINT
                released_.set(b);
            }
            if (compaction) {
                // decrease the number of acquired blocks if the tail of acquired blocks are released
                for (std::size_t i = 0, n = acquired_; i < n; ++i) {
                    auto b = n - i - 1;
                    if (!released_.test(b)) {
                        break;
                    }
                    released_.reset(b);
                    --acquired_;
                }
            }
        }
    private:
        page_pool::page_info head_ {};
        std::size_t acquired_ { 0 };
        std::bitset<nblocks_in_page> released_ {};
    };

    page_pool* page_pool_;
    bool reuse_page_;
    std::map<page_pool::page_info, block_info> blocks_ {};
    block_info* active_ {};
};

} // namespace jogasaki::memory
