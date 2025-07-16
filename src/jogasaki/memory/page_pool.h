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

#include <cstdlib>
#include <iostream>
#include <mutex>
#include <numa.h>
#include <sched.h>
#include <utility>
#include <vector>
#include <boost/container/pmr/memory_resource.hpp>
#include <sys/mman.h>
#include <tbb/concurrent_queue.h>

#include <jogasaki/utils/interference_size.h>

namespace jogasaki::memory {

/*
 * @brief default page size used by page_pool
 */
constexpr static std::size_t page_size = 2UL * 1024UL * 1024UL;

/**
 * @brief page pool
 * @details page pool is a source of fixed length large memory buffers( "pages" )
 * Pages are provided to paged_memory_resource so that it can conduct more granular memory management.
 * After using the page, paged_memory_resource returns pages to page pool and page pool try to recycle
 * the returned page efficiently.
 */
class cache_align page_pool {
public:
    /**
     * @brief page information
     */
    class page_info {
      public:
        /*
         * @brief Value indicating that the node number is not defined
         */
        constexpr static std::size_t undefined_numa_node = -1;
        /**
         * @brief construct with no param
         */
        page_info() = default;
        /**
         * @brief construct with address and node number where the page is created
         */
        constexpr page_info(void *address, std::size_t birth_place) noexcept :
            address_(address), birth_place_(birth_place)
        {}

        /**
         * @brief return true if this contains valid page
         */
        [[nodiscard]] explicit operator bool() const noexcept { return address_ != nullptr; }

        /**
         * @brief operator for sorting the pages in address order
         */
        friend bool operator<(const page_info one, const page_info other) noexcept {
            return one.address_ < other.address_;
        }

        /**
         * @brief return the page address
         */
        [[nodiscard]] void* address() const noexcept { return address_; }

        /**
         * @brief return the node number where the page is created
         */
        [[nodiscard]] std::size_t birth_place() const noexcept { return birth_place_; }

      private:
        void *address_{};
        std::size_t birth_place_{undefined_numa_node};
    };

    /**
     * @brief minimum alignment of a page
     */
    constexpr static std::size_t min_alignment = 4*1024UL;
    /**
     * @brief construct
     */
    page_pool();
    /**
     * @brief copy construct
     */
    page_pool(page_pool const& other) = delete;
    /**
     * @brief move construct
     */
    page_pool(page_pool&& other) = delete;
    /**
     * @brief copy assign
     */
    page_pool& operator=(page_pool const& other) = delete;
    /**
     * @brief move assign
     */
    page_pool& operator=(page_pool&& other) = delete;

    /**
     * @brief destruct page_pool
     */
    ~page_pool();

    /**
     * @brief acquire page from the pool
     * @param brandnew if true, page will always be newly created
     * @return page info to the acquired pool
     * @return rv.page_ is nullptr if page allocation failed
     */
    [[nodiscard]] page_info acquire_page(bool brandnew = false);

    /**
     * @brief release page to the pool
     * @param page page info retrieved from the pool by calling acquire()
     */
    void release_page(page_info page) noexcept;

    /**
     * @brief dump pool information
     * @details this is thread-unsafe operation and can break running sql if any.
     * Appropriate for debugging on system failure (e.g. freeze) or on database shutdown.
     * @param out the output stream to be written to
     */
    void unsafe_dump_info(std::ostream& out);

private:
    using free_pages_type = tbb::concurrent_queue<void*>;
    std::vector<free_pages_type> free_pages_vector_{};

    std::size_t node_num() {
        if(int cpu = sched_getcpu(); cpu >= 0) {
            if (int node = numa_node_of_cpu(cpu); node >= 0) {
                return node;
            }
            // WSL2 uses kernel with no numa support and -1 is returned from numa_node_of_cpu()
            // Treat as a single node.
            return 0;
        }
        std::abort();
    }
    free_pages_type& get_free_pages() {
        return free_pages_vector_.at(node_num());
    }
    free_pages_type& get_free_pages(std::size_t node) {
        return free_pages_vector_.at(node);
    }

};

}
