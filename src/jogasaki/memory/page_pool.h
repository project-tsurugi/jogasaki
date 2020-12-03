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

#include <iostream>
#include <vector>
#include <mutex>
#include <sys/mman.h>
#include <numa.h>
#include <sched.h>

#include <boost/container/pmr/memory_resource.hpp>

#include <jogasaki/utils/interference_size.h>

namespace jogasaki::memory {

/*
 * @brief default page size used by page_pool
 */
constexpr static std::size_t page_size = 2*1024*1024;

/**
 * @brief page pool
 * @details page pool is a source of fixed length large memory buffers( "pages" )
 * Pages are provided to paged_memory_resource so that it can conduct more granular memory management.
 * After using the page, paged_memory_resource returns pages to page pool and page pool try to recycle the returned page efficiently.
 */
class cache_align page_pool {
public:
    /**
     * @brief page information
     */
    class page_info {
      public:
        page_info(void *address, std::size_t birth_place) : address_(address), birth_place_(birth_place) {}
        page_info() : address_(nullptr), birth_place_(-1) {}
        friend bool operator<(const page_info one, const page_info other) { return one.address_ < other.address_; }
        void address(void *address) { address_ = address; }
        void* address() const { return address_; }
        std::size_t birth_place() const { return birth_place_; }
      private:
        void *address_{};
        std::size_t birth_place_{};
    };

    /**
     * @brief minimum alignment of a page
     */
    constexpr static std::size_t min_alignment = 4*1024;
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

private:
    std::vector<std::vector<void *>> free_pages_vector_{};
    std::mutex page_mtx_{};

    std::size_t node_num() {
        if(int cpu = sched_getcpu(); cpu >= 0) {
            if (int node = numa_node_of_cpu(cpu); node >= 0) {
                return node;
            }
        }
        std::abort();
    }
    std::vector<void *>& get_free_pages() {
        return free_pages_vector_.at(node_num());
    }
    std::vector<void *>& get_free_pages(std::size_t node) {
        return free_pages_vector_.at(node);
    }

};

}
