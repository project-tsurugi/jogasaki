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

TEST_F(checkpoint_holder_test, defer_no_set_checkpoint_is_noop) {
    // defer=true without set_checkpoint(): dtor must be a no-op
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    {
        checkpoint_holder cp{&resource, true};
        resource.allocate(page_size * 2 / 3);
        ASSERT_GT(page_size / 2, resource.page_remaining());
        // cp goes out of scope without set_checkpoint() - should not deallocate
    }
    ASSERT_GT(remaining, resource.page_remaining());
}

TEST_F(checkpoint_holder_test, defer_with_set_checkpoint) {
    // defer=true, then set_checkpoint() before dtor: should deallocate
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    std::size_t remaining_at_checkpoint = 0;
    {
        checkpoint_holder cp{&resource, true};
        resource.allocate(page_size / 4);
        remaining_at_checkpoint = resource.page_remaining();
        cp.set_checkpoint();
        resource.allocate(page_size / 4);
        ASSERT_GT(remaining, resource.page_remaining());
    }
    // after dtor, should be back to the point where set_checkpoint() was called
    ASSERT_EQ(remaining_at_checkpoint, resource.page_remaining());
}

TEST_F(checkpoint_holder_test, reset_then_reuse) {
    // reset() clears checkpoint_set_; set_checkpoint() and reset() can be called again
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    checkpoint_holder cp{&resource};

    resource.allocate(page_size / 4);
    cp.reset();
    ASSERT_EQ(remaining, resource.page_remaining());

    // reuse: set new checkpoint and allocate again
    cp.set_checkpoint();
    resource.allocate(page_size / 4);
    ASSERT_GT(remaining, resource.page_remaining());
    cp.reset();
    ASSERT_EQ(remaining, resource.page_remaining());
}

TEST_F(checkpoint_holder_test, set_checkpoint_idempotent) {
    // calling set_checkpoint() multiple times should use the first checkpoint
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    {
        checkpoint_holder cp{&resource, true};
        cp.set_checkpoint();
        resource.allocate(page_size / 4);
        cp.set_checkpoint(); // second call should be ignored
        resource.allocate(page_size / 4);
    }
    // should restore to the point of the first set_checkpoint()
    ASSERT_EQ(remaining, resource.page_remaining());
}


}

