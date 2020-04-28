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
#include <memory/block_memory_resource.h>

#include <gtest/gtest.h>

namespace jogasaki::memory {

class block_memory_resource_test : public ::testing::Test {
public:
    using bmr = block_memory_resource<65536>;
    page_pool pages_;
    static constexpr auto align = alignof(std::max_align_t);
    static constexpr auto no_active_page = paged_memory_resource::unknown_size;
};

TEST_F(block_memory_resource_test, simple) {
    bmr r { pages_ };

    void* p = r.allocate(100);
    ASSERT_TRUE(p);
    r.deallocate(p, 100);
}

TEST_F(block_memory_resource_test, blocks) {
    bmr r { pages_ };

    EXPECT_EQ(r.page_remaining(align), no_active_page);

    auto sz = 100;
    r.allocate(sz);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 1);
    r.allocate(sz);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 2);
    r.allocate(sz);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 3);
}

TEST_F(block_memory_resource_test, blocks_lim) {
    bmr r { pages_ };

    auto sz = bmr::block_size;

    r.allocate(sz);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 1);
    r.allocate(sz);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 2);
    r.allocate(sz);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 3);
}

TEST_F(block_memory_resource_test, blocks_exceed) {
    bmr r { pages_ };

    auto sz = bmr::block_size + 1;

    r.allocate(sz);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 2);
    r.allocate(sz);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 4);
    r.allocate(sz);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 6);
}

TEST_F(block_memory_resource_test, new_page) {
    bmr r { pages_ };

    for (std::size_t i = 0; i < bmr::nblocks_in_page; ++i) {
        r.allocate(1);
    }
    EXPECT_EQ(r.page_remaining(align), 0);

    r.allocate(1);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 1);

    r.allocate(1);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 2);
}

TEST_F(block_memory_resource_test, reuse_current) {
    bmr r { pages_, true };

    auto sz = bmr::block_size + 1;
    void* p = r.allocate(sz);
    ASSERT_TRUE(p);
    EXPECT_EQ(r.count_pages(), 1);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 2);

    r.deallocate(p, sz);
    EXPECT_EQ(r.count_pages(), 1);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 0);
}

TEST_F(block_memory_resource_test, reuse_past) {
    bmr r { pages_, true };

    auto sz = bmr::block_size * (bmr::nblocks_in_page - 1) + 1;
    void* p1 = r.allocate(bmr::block_size * (bmr::nblocks_in_page - 1) + 1);
    EXPECT_EQ(r.count_pages(), 1);

    r.allocate(100);
    EXPECT_EQ(r.count_pages(), 2);

    r.deallocate(p1, sz);
    EXPECT_EQ(r.count_pages(), 2);
    EXPECT_EQ(r.page_remaining(align), page_size);

    void* p3 = r.allocate(100);
    EXPECT_EQ(r.count_pages(), 2);
    EXPECT_EQ(p1, p3);
}

TEST_F(block_memory_resource_test, release_current) {
    bmr r { pages_, false };

    auto sz = bmr::block_size + 1;
    void* p = r.allocate(sz);
    ASSERT_TRUE(p);
    EXPECT_EQ(r.count_pages(), 1);
    EXPECT_EQ(r.page_remaining(align), page_size - bmr::block_size * 2);

    r.deallocate(p, sz);
    EXPECT_EQ(r.count_pages(), 0);
    EXPECT_EQ(r.page_remaining(align), no_active_page);
}

TEST_F(block_memory_resource_test, release_past) {
    bmr r { pages_, false };

    auto sz = bmr::block_size * (bmr::nblocks_in_page - 1) + 1;
    void* p1 = r.allocate(bmr::block_size * (bmr::nblocks_in_page - 1) + 1);

    void* p2 = r.allocate(100);
    EXPECT_EQ(r.count_pages(), 2);

    r.deallocate(p1, sz);
    EXPECT_EQ(r.count_pages(), 1);

    void* p3 = r.allocate(100);
    EXPECT_EQ(r.count_pages(), 1);
    EXPECT_EQ(static_cast<char*>(p2) + bmr::block_size, p3);
}

TEST_F(block_memory_resource_test, release_fifo) {
    bmr r { pages_, false };

    std::vector<void*> blocks;
    for (std::size_t i = 0; i < bmr::nblocks_in_page; ++i) {
        void* p = r.allocate(1);
        ASSERT_TRUE(p);
        EXPECT_EQ(r.count_pages(), 1);
        blocks.emplace_back(p);
    }

    for (std::size_t i = 0; i < bmr::nblocks_in_page; ++i) {
        EXPECT_EQ(r.count_pages(), 1);
        r.deallocate(blocks[i], 1);
    }
    EXPECT_EQ(r.count_pages(), 0);
}

TEST_F(block_memory_resource_test, release_lifo) {
    bmr r { pages_, false };

    std::vector<void*> blocks;
    for (std::size_t i = 0; i < bmr::nblocks_in_page; ++i) {
        void* p = r.allocate(1);
        ASSERT_TRUE(p);
        EXPECT_EQ(r.count_pages(), 1);
        blocks.emplace_back(p);
    }

    for (std::size_t i = 0; i < bmr::nblocks_in_page; ++i) {
        EXPECT_EQ(r.count_pages(), 1);
        r.deallocate(blocks[bmr::nblocks_in_page - i - 1], 1);
    }
    EXPECT_EQ(r.count_pages(), 0);
}

TEST_F(block_memory_resource_test, release_rem) {
    bmr r { pages_, false };

    std::vector<void*> blocks;
    for (std::size_t i = 0; i < bmr::nblocks_in_page; ++i) {
        void* p = r.allocate(1);
        ASSERT_TRUE(p);
        EXPECT_EQ(r.count_pages(), 1);
        blocks.emplace_back(p);
    }

    for (std::size_t i = 0; i < bmr::nblocks_in_page; ++i) {
        EXPECT_EQ(r.count_pages(), 1);
        r.deallocate(blocks[i * 13 % bmr::nblocks_in_page], 1);
    }
    EXPECT_EQ(r.count_pages(), 0);
}

} // dc::memory
