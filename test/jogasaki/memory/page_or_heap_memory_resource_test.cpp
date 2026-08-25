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
#include <cstddef>
#include <cstring>
#include <vector>
#include <boost/container/pmr/polymorphic_allocator.hpp>
#include <gtest/gtest.h>

#include <jogasaki/memory/page_or_heap_memory_resource.h>
#include <jogasaki/memory/page_pool.h>

namespace jogasaki::testing {

using namespace jogasaki::memory;

class page_or_heap_memory_resource_test : public ::testing::Test {};

TEST_F(page_or_heap_memory_resource_test, small_allocations_come_from_a_page) {
    page_pool pool{};
    page_or_heap_memory_resource resource{&pool};
    EXPECT_EQ(0, resource.count_pages());

    auto* p0 = resource.allocate(8, 8);
    ASSERT_NE(nullptr, p0);
    EXPECT_EQ(1, resource.count_pages());

    // successive small allocations are served from the same page and are contiguous
    auto* p1 = resource.allocate(8, 8);
    ASSERT_NE(nullptr, p1);
    EXPECT_EQ(1, resource.count_pages());
    EXPECT_EQ(static_cast<std::byte*>(p0) + 8, static_cast<std::byte*>(p1));
}

TEST_F(page_or_heap_memory_resource_test, page_sized_allocation_fits_in_a_page) {
    page_pool pool{};
    page_or_heap_memory_resource resource{&pool};

    // page_size is the boundary: it must still be served from the pool
    auto* p = resource.allocate(page_size, 8);
    ASSERT_NE(nullptr, p);
    EXPECT_EQ(1, resource.count_pages());
    std::memset(p, 0, page_size);  // the whole block must be usable

    // the page is now exhausted, so the next allocation takes a new page
    auto* q = resource.allocate(page_size, 8);
    ASSERT_NE(nullptr, q);
    EXPECT_EQ(2, resource.count_pages());
    resource.deallocate(p, page_size, 8);
    resource.deallocate(q, page_size, 8);
}

TEST_F(page_or_heap_memory_resource_test, over_page_allocation_goes_to_heap) {
    page_pool pool{};
    page_or_heap_memory_resource resource{&pool};

    constexpr std::size_t sz = page_size + 1;
    auto* p = resource.allocate(sz, 8);
    ASSERT_NE(nullptr, p);
    std::memset(p, 0, sz);
    // no page was acquired from the pool for this one
    EXPECT_EQ(0, resource.count_pages());

    // heap allocations are actually reclaimed, so repeating alloc/dealloc does not grow the pages
    resource.deallocate(p, sz, 8);
    for (std::size_t i = 0; i < 100; ++i) {
        auto* q = resource.allocate(sz, 8);
        ASSERT_NE(nullptr, q);
        resource.deallocate(q, sz, 8);
    }
    EXPECT_EQ(0, resource.count_pages());
}

TEST_F(page_or_heap_memory_resource_test, mixed_sizes) {
    page_pool pool{};
    page_or_heap_memory_resource resource{&pool};

    auto* small = resource.allocate(1024, 8);
    ASSERT_NE(nullptr, small);
    EXPECT_EQ(1, resource.count_pages());

    auto* large = resource.allocate(page_size * 4, 8);
    ASSERT_NE(nullptr, large);
    EXPECT_EQ(1, resource.count_pages());
    std::memset(large, 0, page_size * 4);

    // the small allocation is untouched by the large one
    std::memset(small, 1, 1024);
    EXPECT_EQ(std::byte{1}, *static_cast<std::byte*>(small));

    resource.deallocate(large, page_size * 4, 8);
    resource.deallocate(small, 1024, 8);
}

TEST_F(page_or_heap_memory_resource_test, is_equal) {
    page_pool pool{};
    page_or_heap_memory_resource r0{&pool};
    page_or_heap_memory_resource r1{&pool};
    EXPECT_TRUE(r0.is_equal(r0));
    EXPECT_FALSE(r0.is_equal(r1));
}

TEST_F(page_or_heap_memory_resource_test, growing_container) {
    // emulates the count_distinct usage: a container whose storage is a single contiguous block
    // that is relocated as it grows, crossing the page boundary in the middle
    page_pool pool{};
    page_or_heap_memory_resource resource{&pool};

    std::vector<std::int64_t, boost::container::pmr::polymorphic_allocator<std::int64_t>> v{
        boost::container::pmr::polymorphic_allocator<std::int64_t>{&resource}};
    constexpr std::size_t n = (page_size / sizeof(std::int64_t)) * 3;
    for (std::size_t i = 0; i < n; ++i) {
        v.emplace_back(static_cast<std::int64_t>(i));
    }
    ASSERT_EQ(n, v.size());
    EXPECT_GT(v.capacity() * sizeof(std::int64_t), page_size);
    for (std::size_t i = 0; i < n; ++i) {
        ASSERT_EQ(static_cast<std::int64_t>(i), v[i]) << "index:" << i;
    }
}

}  // namespace jogasaki::testing
