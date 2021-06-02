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

memory::page_pool::page_pool() {
    auto nodes = numa_num_configured_nodes();
    if (nodes == 0) {
        // WSL2 uses kernel with no numa support and 0 is returned. Treat as a single node.
        nodes = 1;
    }
    free_pages_vector_.resize(nodes);
}

memory::page_pool::~page_pool() {
    for (const auto& free_pages_ : free_pages_vector_) {
        for (auto b = free_pages_.unsafe_begin(), e = free_pages_.unsafe_end(); b != e; ++b) {
            if (munmap(*b, page_size) < 0) {
                std::abort();
            }
        }
    }
}

page_pool::page_info memory::page_pool::acquire_page(bool brandnew) {
    void* page{};
    std::size_t node = node_num();
    if (!brandnew) {
        auto& free_pages = get_free_pages(node);
        if(free_pages.try_pop(page)) {
            return page_info(page, node);
        }
    }
    page = mmap(nullptr, page_size, PROT_READ | PROT_WRITE, //NOLINT
        (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB), -1, 0); //NOLINT
    if (page == MAP_FAILED) { //NOLINT
        page = mmap(nullptr, page_size, PROT_READ | PROT_WRITE, //NOLINT
            (MAP_PRIVATE | MAP_ANONYMOUS), -1, 0); //NOLINT
        if (page == MAP_FAILED) { //NOLINT
            return page_info(nullptr, node);
        }
    }
    return page_info(page, node);
}

void page_pool::release_page(page_info page) noexcept {
    auto& free_pages = get_free_pages(page.birth_place());
    free_pages.push(page.address());
}

}
