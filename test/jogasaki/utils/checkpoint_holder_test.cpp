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
#include <memory>
#include <gtest/gtest.h>

#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/utils/checkpoint_holder.h>

namespace jogasaki::utils {

class checkpoint_holder_test : public ::testing::Test {};

using namespace memory;

TEST_F(checkpoint_holder_test, simple) {
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    {
        checkpoint_holder cp{&resource};
        resource.allocate(page_size * 2 / 3);
        ASSERT_GT(page_size / 2, resource.page_remaining());
    }
    ASSERT_EQ(remaining, resource.page_remaining());
}

TEST_F(checkpoint_holder_test, checkpoint_at_beginning_of_page) {
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    std::size_t remaining = resource.page_remaining();
    {
        checkpoint_holder cp{&resource};
        resource.allocate(page_size * 2 / 3);
        ASSERT_GT(page_size / 2, resource.page_remaining());
    }
    ASSERT_EQ(remaining, resource.page_remaining());
}


}

