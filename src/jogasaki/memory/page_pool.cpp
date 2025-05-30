/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <string>
#include <type_traits>
#include <glog/logging.h>
#include <nlohmann/json.hpp>
#include <sys/mman.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>


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
            return {page, node};
        }
    }
    page = mmap(nullptr, page_size, PROT_READ | PROT_WRITE, //NOLINT
        (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB), -1, 0); //NOLINT
    if (page == MAP_FAILED) { //NOLINT
        LOG_FIRST_N(INFO, 1) << "SQL engine page pool uses non-huge pages page_size:" << page_size;
        page = mmap(nullptr, page_size, PROT_READ | PROT_WRITE, //NOLINT
            (MAP_PRIVATE | MAP_ANONYMOUS), -1, 0); //NOLINT
        if (page == MAP_FAILED) { //NOLINT
            LOG_LP(ERROR) << "memory allocation failed page_size:" << page_size << " node:" << node;
            return {nullptr, node};
        }
    }
    return {page, node};
}

void page_pool::release_page(page_info page) noexcept {
    if(global::config_pool()->return_os_pages()) {
        if(0 != munmap(page.address(), page_size)) {
            LOG_LP(ERROR) << "internal error - munmap failed << " << page.address();
        }
        return;
    }
    auto& free_pages = get_free_pages(page.birth_place());
    free_pages.push(page.address());
}

void page_pool::unsafe_dump_info(std::ostream& out) {
    using json = nlohmann::json;
    try {
        json j{};
        j["nodes"] = json::array();
        auto& nodes = j["nodes"];
        std::size_t id = 0;
        for(auto&& e: free_pages_vector_) {
            json node{};
            node["id"] = id++;
            // tbb::concurrent_queue::unsafe_size() sometime returns 0, so we avoid using it here
            std::size_t sz = 0;
            for(auto it = e.unsafe_begin(), end = e.unsafe_end(); it != end; ++it) {
                ++sz;
            }
            node["free_page_count"] = sz;
            node["free_page_bytes"] = sz * page_size;
            nodes.emplace_back(std::move(node));
        }
        out << j.dump();
    } catch (json::exception const& e) {
        VLOG_LP(log_error) << "json exception on dumping page pool information " << e.what();
    }
}

}
