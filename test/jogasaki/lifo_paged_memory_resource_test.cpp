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
#include "memory/lifo_paged_memory_resource.h"

#include <gtest/gtest.h>
#include <boost/container/pmr/polymorphic_allocator.hpp>

const std::size_t loop =  50;

template <typename T>
using PmrAllocator = boost::container::pmr::polymorphic_allocator<T>;

using ByteArray = std::array<std::byte, 1024*1024>;
using ByteArrayAllocator = PmrAllocator<ByteArray>;

namespace jogasaki::testing {

class lifo_paged_memory_resource_test : public ::testing::Test {
public:
};

TEST_F(lifo_paged_memory_resource_test, basic) {
    auto my_pool = std::make_unique<memory::page_pool>();
    auto my_resource = std::make_unique<memory::lifo_paged_memory_resource>(my_pool.get());
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

    // release in LIFO order
    for(std::size_t i = 0; i < loop; i++) {
        my_allocator.deallocate(blocks[(loop - 1) - i], 1);
        EXPECT_EQ(my_resource->count_pages(), (loop / 2) - ((i+1) / 2));
    }
}

TEST_F(lifo_paged_memory_resource_test, deallocate_after_at_even) {
    auto my_pool = std::make_unique<memory::page_pool>();
    auto my_resource = std::make_unique<memory::lifo_paged_memory_resource>(my_pool.get());
    EXPECT_TRUE(my_pool);
    EXPECT_TRUE(my_resource);

    ByteArrayAllocator my_allocator(my_resource.get());
    std::size_t count, remaining;

    memory::lifo_paged_memory_resource::checkpoint point;
    for(std::size_t i = 0; i < loop; i++) {
        my_allocator.allocate(1);
        if (i == loop / 2) {
            point = my_resource->get_checkpoint();
            count = my_resource->count_pages();
            remaining = my_resource->page_remaining();
        }
    }

    // deallocate to the checkpoint
    my_resource->deallocate_after(point);
    EXPECT_EQ(my_resource->count_pages(), count);
    EXPECT_EQ(my_resource->page_remaining(), remaining);
}

TEST_F(lifo_paged_memory_resource_test, deallocate_after_at_odd) {
    auto my_pool = std::make_unique<memory::page_pool>();
    auto my_resource = std::make_unique<memory::lifo_paged_memory_resource>(my_pool.get());
    EXPECT_TRUE(my_pool);
    EXPECT_TRUE(my_resource);

    ByteArrayAllocator my_allocator(my_resource.get());
    std::size_t count, remaining;

    memory::lifo_paged_memory_resource::checkpoint point;
    for(std::size_t i = 0; i < loop; i++) {
        my_allocator.allocate(1);
        if (i == loop / 2 - 1) {
            point = my_resource->get_checkpoint();
            count = my_resource->count_pages();
            remaining = my_resource->page_remaining();
        }
    }

    // deallocate to the checkpoint
    my_resource->deallocate_after(point);
    EXPECT_EQ(my_resource->count_pages(), count);
    EXPECT_EQ(my_resource->page_remaining(), remaining);
}

TEST_F(lifo_paged_memory_resource_test, end_current_page) {
    auto my_pool = std::make_unique<memory::page_pool>();
    auto my_resource = std::make_unique<memory::lifo_paged_memory_resource>(my_pool.get());
    EXPECT_TRUE(my_pool);
    EXPECT_TRUE(my_resource);

    ByteArrayAllocator my_allocator(my_resource.get());

    ByteArray *blocks[2];

    blocks[0] = my_allocator.allocate(1);
    my_resource->end_current_page();
    EXPECT_TRUE(my_resource->page_remaining() == 0 || my_resource->page_remaining() == jogasaki::memory::page_size);
    blocks[1] = my_allocator.allocate(1);
    EXPECT_EQ(my_resource->count_pages(), 2);

    my_allocator.deallocate(blocks[1], 1);
    EXPECT_EQ(my_resource->count_pages(), 1);
}

}
