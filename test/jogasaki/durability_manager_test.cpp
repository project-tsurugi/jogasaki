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
#include <jogasaki/durability_manager.h>

#include <gtest/gtest.h>
#include <jogasaki/error_code.h>
#include <jogasaki/error/error_info_factory.h>

namespace jogasaki {

class durability_manager_test : public ::testing::Test {};

TEST_F(durability_manager_test, basic) {
    durability_manager mgr{};
    auto tx0 = std::make_shared<transaction_context>();
    tx0->durability_marker(0);
    auto tx1 = std::make_shared<transaction_context>();
    tx1->durability_marker(1);
    auto tx2 = std::make_shared<transaction_context>();
    tx2->durability_marker(2);
    mgr.add(tx0);
    mgr.add(tx1);
    mgr.add(tx2);
    std::atomic_bool called = false;
    std::shared_ptr<transaction_context> tx{};
    auto cb = [&](std::shared_ptr<transaction_context> const& t) {
        called = true;
        tx = t;
    };
    mgr.update_durability_marker(0, cb);
    ASSERT_TRUE(called);
    EXPECT_EQ(tx0, tx);
    called = false;
    tx = {};
    mgr.update_durability_marker(1, cb);
    ASSERT_TRUE(called);
    EXPECT_EQ(tx1, tx);
    called = false;
    tx = {};
    mgr.update_durability_marker(2, cb);
    ASSERT_TRUE(called);
    EXPECT_EQ(tx2, tx);
}

}

