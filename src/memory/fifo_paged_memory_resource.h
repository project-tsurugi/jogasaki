#pragma once

#include <algorithm>
#include <deque>

#include "paged_memory_resource.h"
#include "page_pool.h"
#include "details/page_allocation_info.h"

namespace jogasaki::memory {

/**
 * @brief an implementation of paged_memory_resource that only can deallocate memory fragments by FIFO order.
 */
class fifo_paged_memory_resource : public paged_memory_resource {
public:
    /**
     * @brief creates a new instance.
     * @param pool the parent page pool
     */
    explicit fifo_paged_memory_resource(page_pool* pool)
        : page_pool_(pool)
    {}

    ~fifo_paged_memory_resource() override {
        for (const auto& p : pages_) {
            page_pool_->release_page(p.head());
        }
    }

    fifo_paged_memory_resource(fifo_paged_memory_resource const& other) = delete;
    fifo_paged_memory_resource(fifo_paged_memory_resource&& other) = delete;
    fifo_paged_memory_resource& operator=(fifo_paged_memory_resource const& other) = delete;
    fifo_paged_memory_resource& operator=(fifo_paged_memory_resource&& other) = delete;

    /**
     * @brief returned the number of holding pages.
     * @return the number of holding pages
     */
    [[nodiscard]] std::size_t count_pages() const noexcept {
        return pages_.size();
    }

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
    [[nodiscard]] checkpoint get_checkpoint() const noexcept {
        if (pages_.empty()) {
            return { nullptr, 0 };
        }
        auto& current = pages_.back();
        return { current.head(), current.upper_bound_offset() };
    }

    /**
     * @brief releases the allocated region before the given checkpoint.
     * @param point the checkpoint
     */
    void deallocate_before(checkpoint const& point) {
        if (point.head_ == nullptr) {
            return;
        }
        while (!pages_.empty()) {
            auto&& page = pages_.front();
            if (page.head() == point.head_) {
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

    void end_current_page() noexcept override {
        if (!pages_.empty()) {
            if (pages_.back().remaining(1) == page_size) {
                return;
            }
        }
        // allocate a new page
        acquire_new_page();
    }

protected:
    /**
     * @brief allocates a new buffer.
     * @param bytes the required buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     * @return pointer to the allocated buffer
     * @throws std::bad_alloc if allocation was failed
     */
    void* do_allocate(std::size_t bytes, std::size_t alignment) override {
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

    /**
     * @brief deallocates the buffer previously allocated by this resource.
     * @details this only can deallocate by FIFO order, that is, this function only accepts the eldest
     *      memory region which has not yet been deallocated.
     * @param p pointer to the buffer to be deallocated
     * @param bytes the buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     * @warning undefined behavior if the given memory fragment is not the eldest one
     */
    void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
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

    [[nodiscard]] bool do_is_equal(const memory_resource& other) const noexcept override {
        return this == &other;
    }

    [[nodiscard]] std::size_t do_page_remaining(std::size_t alignment) const noexcept override {
        if (pages_.empty()) {
            return 0;
        }
        return pages_.back().remaining(alignment);
    }

private:
    page_pool *page_pool_{};
    std::deque<details::page_allocation_info> pages_{};

    details::page_allocation_info& acquire_new_page() {
        void* new_page = page_pool_->acquire_page();
        if (new_page == nullptr) {
            throw std::bad_alloc();
        }
        return pages_.emplace_back(new_page);
    }
};

} // dc::memory
