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

#include <boost/container/pmr/memory_resource.hpp>

namespace jogasaki::memory {

/**
 * @brief page based memory resource
 * @details paged memory resource manages pages borrowed from page_pool and provide smaller piece of memory region
 * via memory_resource interface.
 * Subclasses should implement protected member functions of memory_resource (e.g. do_allocate) to allocate/de-allocate
 * memory region from the current page managed by this instance.
 * When paged_memory_resource is destructed, all the storage borrowed from page_pool are returned.
 * This is different from general memory_resource, whose destruction doesn't necessarily return
 * allocated resource (e.g. new_delete_resource)
 * Subclasses are required to implement so that allocate(m) is successful as long as m is equal or less than page_size.
 * This interface doesn't assume any allocation/de-allocation patterns by the caller, so subclasses can implement
 * optimized logic specific to their purpose based on access patterns such as FIFO/LIFO.
 */
class paged_memory_resource : public boost::container::pmr::memory_resource {
public:
    /**
     * @brief unknown size constant
     */
    constexpr static inline std::size_t unknown_size = std::size_t(-1);

    /**
     * @brief retrieve remaining bytes in the current page
     * @return buffer size in bytes
     * @return unknown_size if the remaining bytes is unknown, or current page is not active (i.e. not allocated yet)
     */
    [[nodiscard]] std::size_t page_remaining(std::size_t alignment = 1U) const noexcept {
        return do_page_remaining(alignment);
    }

    /**
     * @brief finish using current page
     * By this function, caller can request stop using the current page and allocate from new one when further
     * allocation is requested. This is useful when the caller knows the remaining bytes in the current page is
     * too small and allocating from the page is not efficient.
     * @note This function is no-op if current page is not active.
     * @note If no allocation has been made from the current page, this function can be no-op.
     */
    virtual void end_current_page() = 0;

protected:
    /**
     * @brief subclass implementation of page_remaining
     * @see page_remaining()
     */
    [[nodiscard]] virtual std::size_t do_page_remaining(std::size_t alignment) const noexcept = 0;

};

} // namespace jogasaki::memory
