/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <gtest/gtest.h>

#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/memory/paged_memory_resource.h>
#include <jogasaki/utils/lazy_checkpoint_holder.h>

namespace jogasaki::utils {

class lazy_checkpoint_holder_test : public ::testing::Test {};

using namespace memory;

TEST_F(lazy_checkpoint_holder_test, no_set_checkpoint_dtor_noop) {
    // without set_checkpoint(), dtor must not free any memory
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    {
        lazy_checkpoint_holder cp{&resource};
        resource.allocate(page_size * 2 / 3);
        ASSERT_GT(remaining, resource.page_remaining());
        // cp goes out of scope without set_checkpoint() - must not deallocate
    }
    ASSERT_GT(remaining, resource.page_remaining());
}

TEST_F(lazy_checkpoint_holder_test, release_frees_but_keeps_checkpoint) {
    // release() frees back to the checkpoint but keeps it armed
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    lazy_checkpoint_holder cp{&resource};
    cp.set_checkpoint();

    resource.allocate(page_size / 4);
    ASSERT_GT(remaining, resource.page_remaining());
    cp.release();
    ASSERT_EQ(remaining, resource.page_remaining());

    // checkpoint is still armed: a second release also frees back to the same position
    resource.allocate(page_size / 4);
    cp.release();
    ASSERT_EQ(remaining, resource.page_remaining());
}

TEST_F(lazy_checkpoint_holder_test, release_without_set_checkpoint_is_noop) {
    // release() before set_checkpoint() must not free any memory
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    lazy_checkpoint_holder cp{&resource};
    resource.allocate(page_size / 4);
    ASSERT_GT(remaining, resource.page_remaining());
    cp.release();  // no checkpoint set - no-op
    ASSERT_GT(remaining, resource.page_remaining());
}

TEST_F(lazy_checkpoint_holder_test, reset_releases_and_disarms) {
    // reset() = release() + unset(): frees memory and disarms the checkpoint
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    lazy_checkpoint_holder cp{&resource};
    cp.set_checkpoint();
    resource.allocate(page_size / 4);
    ASSERT_GT(remaining, resource.page_remaining());
    cp.reset();
    ASSERT_EQ(remaining, resource.page_remaining());

    // checkpoint is now disarmed: set_checkpoint() may be called again
    cp.set_checkpoint();
    resource.allocate(page_size / 4);
    cp.reset();
    ASSERT_EQ(remaining, resource.page_remaining());
}

TEST_F(lazy_checkpoint_holder_test, reset_without_set_checkpoint_is_noop) {
    // reset() without a prior set_checkpoint() must not free any memory
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    lazy_checkpoint_holder cp{&resource};
    resource.allocate(page_size / 4);
    cp.reset();  // no checkpoint set - no-op
    ASSERT_GT(remaining, resource.page_remaining());
}

TEST_F(lazy_checkpoint_holder_test, unset_disarms_release_and_reset) {
    // after unset(), both release() and reset() are no-ops
    page_pool pool{};
    lifo_paged_memory_resource resource{&pool};
    resource.allocate(1);
    std::size_t remaining = resource.page_remaining();
    lazy_checkpoint_holder cp{&resource};
    cp.set_checkpoint();
    resource.allocate(page_size / 4);
    std::size_t after_alloc = resource.page_remaining();
    cp.unset();
    cp.release();
    ASSERT_EQ(after_alloc, resource.page_remaining());
    cp.reset();
    ASSERT_EQ(after_alloc, resource.page_remaining());
    // allocated memory is still present
    ASSERT_GT(remaining, resource.page_remaining());
}

}  // namespace jogasaki::utils
