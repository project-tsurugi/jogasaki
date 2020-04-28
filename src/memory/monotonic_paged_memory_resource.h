#pragma once

#include <algorithm>
#include <deque>

#include "paged_memory_resource.h"
#include "page_pool.h"
#include "details/page_allocation_info.h"

namespace dc::memory {

/**
 * @brief an implementation of paged_memory_resource that does not deallocate memory fragments.
 */
class monotonic_paged_memory_resource : public paged_memory_resource {
public:
    /**
     * @brief creates a new instance.
     * @param pool the parent page pool
     */
    explicit monotonic_paged_memory_resource(page_pool* pool)
        : page_pool_(pool)
    {}

    ~monotonic_paged_memory_resource() override {
        for (const auto& p : pages_) {
            page_pool_->release_page(p.head());
        }
    }

    monotonic_paged_memory_resource(monotonic_paged_memory_resource const& other) = delete;
    monotonic_paged_memory_resource(monotonic_paged_memory_resource&& other) = delete;
    monotonic_paged_memory_resource& operator=(monotonic_paged_memory_resource const& other) = delete;
    monotonic_paged_memory_resource& operator=(monotonic_paged_memory_resource&& other) = delete;

    /**
     * @brief returned the number of holding pages.
     * @return the number of holding pages
     */
    [[nodiscard]] std::size_t count_pages() const noexcept {
        return pages_.size();
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
     * @brief do nothing for monotonic memory resource.
     * @details this exists because do_deallocate() is pure virtual of memory_resource.
     * @param p pointer to the buffer to be deallocated
     * @param bytes the buffer size in bytes
     * @param alignment the alignment size of the head of buffer
     */
    void do_deallocate([[maybe_unused]] void* p, [[maybe_unused]] std::size_t bytes, [[maybe_unused]] std::size_t alignment) override {
        // do nothing
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
