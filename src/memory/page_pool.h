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
#include <boost/container/pmr/memory_resource.hpp>

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
class page_pool {
public:
    /**
     * @brief minimum alignment of a page
     */
    constexpr static std::size_t min_alignment = 4*1024;
    /**
     * @brief construct
     */
    page_pool() = default;
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
    ~page_pool() {
        for (const auto& c : free_pages_) {
            if (munmap(c, page_size) < 0) {
                std::abort();
            }
        }
    }

    /**
     * @brief acquire page from the pool
     * @return pointer to the acquired pool
     * @return nullptr if page allocation failed
     */
    void* acquire_page() {
        void* page;
        {
            std::lock_guard<std::mutex> lock(page_mtx_);
            if (!free_pages_.empty()) {
                auto it = free_pages_.begin();
                page = *it;
                free_pages_.erase(it);
                return page;
            }
        }
        page = mmap(nullptr, page_size, PROT_READ | PROT_WRITE, //NOLINT
                    (MAP_SHARED | MAP_ANON | MAP_HUGETLB), -1, 0); //NOLINT
        if (page == MAP_FAILED) {
            page = mmap(nullptr, page_size, PROT_READ | PROT_WRITE, //NOLINT
                        (MAP_SHARED | MAP_ANON), -1, 0); //NOLINT
            if (page == MAP_FAILED) {
                return nullptr;
            }
        }
        return page;
    }

    /**
     * @brief release page to the pool
     * @param page pointer retrieved from the pool by calling acquire()
     */
    void release_page(void* page) noexcept {
        std::lock_guard<std::mutex> lock(page_mtx_);
        free_pages_.emplace_back(page);
    }

private:
    std::vector<void *> free_pages_{};
    std::mutex page_mtx_{};
};

}
