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

#include <gtest/gtest.h>
#include <boost/container/pmr/polymorphic_allocator.hpp>

#include "memory/monotonic_paged_memory_resource.h"

const std::size_t loop =  50;

template <typename T>
using PmrAllocator = boost::container::pmr::polymorphic_allocator<T>;

using ByteArray = std::array<std::byte, 1024*1024>;
using ByteArrayAllocator = PmrAllocator<ByteArray>;

namespace jogasaki::testing {

class monotonic_paged_memory_resource_test : public ::testing::Test {
public:
};

TEST_F(monotonic_paged_memory_resource_test, basic_1) {
    auto my_pool = std::make_unique<memory::page_pool>();
    auto my_resource = std::make_unique<memory::monotonic_paged_memory_resource>(my_pool.get());
    EXPECT_TRUE(my_pool);
    EXPECT_TRUE(my_resource);

    ByteArrayAllocator my_allocator(my_resource.get());

    ByteArray *blocks[loop];
    for(std::size_t i = 0; i < loop; i++) {
        blocks[i] = my_allocator.allocate(1);
        EXPECT_TRUE(blocks[i]);
        EXPECT_EQ(my_resource->count_pages(), i / 2 + 1);
        EXPECT_EQ(my_resource->page_remaining(), i % 2 == 0 ? 1024*1024 : 0);
    }

    std::size_t max_pages = my_resource->count_pages();

    // release in FIFO order
    for(std::size_t i = 0; i < loop; i++) {
        my_allocator.deallocate(blocks[i], 1);
        EXPECT_EQ(my_resource->count_pages(), max_pages);
    }
}

TEST_F(monotonic_paged_memory_resource_test, basic_2) {
    auto my_pool = std::make_unique<memory::page_pool>();
    auto my_resource = std::make_unique<memory::monotonic_paged_memory_resource>(my_pool.get());
    EXPECT_TRUE(my_pool);
    EXPECT_TRUE(my_resource);

    ByteArrayAllocator my_allocator(my_resource.get());

    ByteArray *blocks[loop];
    for(std::size_t i = 0; i < loop; i++) {
        blocks[i] = my_allocator.allocate(1);
        EXPECT_TRUE(blocks[i]);
        EXPECT_EQ(my_resource->count_pages(), i / 2 + 1);
        EXPECT_EQ(my_resource->page_remaining(), i % 2 == 0 ? 1024*1024 : 0);
    }

    std::size_t max_pages = my_resource->count_pages();
    
    // release in LIFO order
    for(std::size_t i = 0; i < loop; i++) {
        my_allocator.deallocate(blocks[(loop - 1) - i], 1);
        EXPECT_EQ(my_resource->count_pages(), max_pages);
    }
}

TEST_F(monotonic_paged_memory_resource_test, end_current_page) {
    auto my_pool = std::make_unique<memory::page_pool>();
    auto my_resource = std::make_unique<memory::monotonic_paged_memory_resource>(my_pool.get());
    EXPECT_TRUE(my_pool);
    EXPECT_TRUE(my_resource);

    ByteArrayAllocator my_allocator(my_resource.get());

    ByteArray *blocks[2];

    blocks[0] = my_allocator.allocate(1);
    my_resource->end_current_page();
    EXPECT_TRUE(my_resource->page_remaining() == 0 || my_resource->page_remaining() == jogasaki::memory::page_size);
    blocks[1] = my_allocator.allocate(1);
    EXPECT_EQ(my_resource->count_pages(), 2);

    my_allocator.deallocate(blocks[0], 1);
    EXPECT_EQ(my_resource->count_pages(), 2);
}

}
