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
#include "page_or_heap_memory_resource.h"

#include <memory>
#include <boost/container/pmr/global_resource.hpp>

#include <jogasaki/memory/page_pool.h>

namespace jogasaki::memory {

std::size_t page_or_heap_memory_resource::count_pages() const noexcept {
    return paged_.count_pages();
}

void* page_or_heap_memory_resource::do_allocate(std::size_t bytes, std::size_t alignment) {
    if (bytes <= page_size) {
        return paged_.allocate(bytes, alignment);
    }
    return boost::container::pmr::new_delete_resource()->allocate(bytes, alignment);
}

void page_or_heap_memory_resource::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
    if (bytes <= page_size) {
        paged_.deallocate(p, bytes, alignment);
        return;
    }
    boost::container::pmr::new_delete_resource()->deallocate(p, bytes, alignment);
}

bool page_or_heap_memory_resource::do_is_equal(memory_resource const& other) const noexcept {
    return this == std::addressof(other);
}

} // namespace jogasaki::memory
