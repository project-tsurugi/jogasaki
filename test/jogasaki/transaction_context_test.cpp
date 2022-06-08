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
#include <jogasaki/transaction_context.h>

#include <gtest/gtest.h>

namespace jogasaki {

class transaction_context_test : public ::testing::Test {};

TEST_F(transaction_context_test, basic) {
    details::worker_manager mgr{};
    EXPECT_EQ(details::worker_manager::empty_worker, mgr.worker_id());
    EXPECT_EQ(0, mgr.use_count());
    {
        std::uint32_t my_worker = 100;
        EXPECT_TRUE(mgr.increment_and_set_on_zero(my_worker));
        EXPECT_EQ(100, mgr.worker_id());
        EXPECT_EQ(1, mgr.use_count());
        EXPECT_EQ(100, my_worker);
    }
    {
        std::uint32_t my_worker = 200;
        EXPECT_FALSE(mgr.increment_and_set_on_zero(my_worker));
        EXPECT_EQ(100, mgr.worker_id());
        EXPECT_EQ(1, mgr.use_count());
        EXPECT_EQ(100, my_worker);
    }
    {
        std::uint32_t my_worker = 100;
        EXPECT_TRUE(mgr.increment_and_set_on_zero(my_worker));
        EXPECT_EQ(100, mgr.worker_id());
        EXPECT_EQ(2, mgr.use_count());
        EXPECT_EQ(100, my_worker);
    }
    {
        EXPECT_FALSE(mgr.decrement_and_clear_on_zero());
        EXPECT_EQ(100, mgr.worker_id());
        EXPECT_EQ(1, mgr.use_count());
    }
    {
        EXPECT_TRUE(mgr.decrement_and_clear_on_zero());
        EXPECT_EQ(details::worker_manager::empty_worker, mgr.worker_id());
        EXPECT_EQ(0, mgr.use_count());
    }
}

}

