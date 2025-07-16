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
#include <string>
#include <utility>
#include <gtest/gtest.h>

#include <jogasaki/durability_manager.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>

namespace jogasaki {

class durability_manager_test : public ::testing::Test {};

std::shared_ptr<request_context> create_rctx(durability_manager::marker_type marker) {
    auto tx = std::make_shared<transaction_context>();
    tx->durability_marker(marker);
    return std::make_shared<request_context>(
        std::shared_ptr<class configuration>{},
        std::shared_ptr<memory::lifo_paged_memory_resource>{},
        std::shared_ptr<kvs::database>{},
        std::move(tx)
    );
}

TEST_F(durability_manager_test, basic) {
    durability_manager mgr{};
    auto rctx0 = create_rctx(0);
    auto rctx1 = create_rctx(1);
    auto rctx2 = create_rctx(2);
    mgr.add_to_waitlist(rctx0);
    mgr.add_to_waitlist(rctx1);
    mgr.add_to_waitlist(rctx2);
    std::atomic_bool called = false;
    std::shared_ptr<request_context> rctx{};
    auto cb = [&](durability_manager::element_reference_type e) {
        called = true;
        rctx = e;
    };
    mgr.update_current_marker(0, cb);
    ASSERT_TRUE(called);
    EXPECT_EQ(rctx0, rctx);
    called = false;
    rctx = {};
    mgr.update_current_marker(1, cb);
    ASSERT_TRUE(called);
    EXPECT_EQ(rctx1, rctx);
    called = false;
    rctx = {};
    mgr.update_current_marker(2, cb);
    ASSERT_TRUE(called);
    EXPECT_EQ(rctx2, rctx);
}

}

