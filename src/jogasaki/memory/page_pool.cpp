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
#include "page_pool.h"

namespace jogasaki::memory {

memory::page_pool::~page_pool() {
    for (const auto& c : free_pages_) {
        if (munmap(c, page_size) < 0) {
            std::abort();
        }
    }
}

void *memory::page_pool::acquire_page() {
    void* page;
    {
        std::lock_guard<std::mutex> lock(page_mtx_);
        if (!free_pages_.empty()) {
            page = free_pages_.back();
            free_pages_.pop_back();
            return page;
        }
    }
    page = mmap(nullptr, page_size, PROT_READ | PROT_WRITE, //NOLINT
        (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB), -1, 0); //NOLINT
    if (page == MAP_FAILED) { //NOLINT
        page = mmap(nullptr, page_size, PROT_READ | PROT_WRITE, //NOLINT
            (MAP_PRIVATE | MAP_ANONYMOUS), -1, 0); //NOLINT
        if (page == MAP_FAILED) { //NOLINT
            return nullptr;
        }
    }
    return page;
}

void page_pool::release_page(void *page) noexcept {
    std::lock_guard<std::mutex> lock(page_mtx_);
    free_pages_.emplace_back(page);
}


}
