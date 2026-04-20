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
#include <thread>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/storage/reference_scope.h>
#include <jogasaki/storage/storage_manager.h>

namespace jogasaki::storage {

class storage_reference_scope_test : public ::testing::Test {};

TEST_F(storage_reference_scope_test, single_storage_referenced_and_released) {
    // ref_transaction_count increments on add and decrements on reference_scope destruction
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));

    {
        reference_scope rs{mgr};
        rs.add_storage(1);
        EXPECT_EQ(1, mgr.find_entry(1)->ref_transaction_count());
    }
    // reference_scope destroyed: count back to 0
    EXPECT_EQ(0, mgr.find_entry(1)->ref_transaction_count());
}

TEST_F(storage_reference_scope_test, add_same_storage_is_idempotent) {
    // adding the same storage multiple times must not inflate the counter
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));

    reference_scope rs{mgr};
    rs.add_storage(1);
    rs.add_storage(1);
    rs.add_storage(1);
    EXPECT_EQ(1, mgr.find_entry(1)->ref_transaction_count());
}

TEST_F(storage_reference_scope_test, multiple_storages) {
    // each distinct storage gets its own counter increment
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));
    ASSERT_TRUE(mgr.add_entry(2, "T2"));

    {
        reference_scope rs{mgr};
        rs.add_storage(1);
        rs.add_storage(2);
        EXPECT_EQ(1, mgr.find_entry(1)->ref_transaction_count());
        EXPECT_EQ(1, mgr.find_entry(2)->ref_transaction_count());
    }
    EXPECT_EQ(0, mgr.find_entry(1)->ref_transaction_count());
    EXPECT_EQ(0, mgr.find_entry(2)->ref_transaction_count());
}

TEST_F(storage_reference_scope_test, multiple_scopes_on_same_storage) {
    // two independent reference_scopes each increment the counter
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));

    reference_scope rs1{mgr};
    rs1.add_storage(1);
    EXPECT_EQ(1, mgr.find_entry(1)->ref_transaction_count());

    {
        reference_scope rs2{mgr};
        rs2.add_storage(1);
        EXPECT_EQ(2, mgr.find_entry(1)->ref_transaction_count());
    }
    // rs2 destroyed
    EXPECT_EQ(1, mgr.find_entry(1)->ref_transaction_count());
}

TEST_F(storage_reference_scope_test, add_nonexistent_storage_is_safe) {
    // adding a storage_entry that is not in the manager must not crash
    storage_manager mgr{};
    reference_scope rs{mgr};
    rs.add_storage(99);  // entry 99 not registered
    // no crash; nothing referenced
}

TEST_F(storage_reference_scope_test, concurrent_add_storage_same_entry) {
    // concurrent add_storage() calls for the same entry must result in count == 1
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));

    reference_scope rs{mgr};
    std::vector<std::thread> threads{};
    std::size_t const n_threads = 8;
    threads.reserve(n_threads);
    for (std::size_t i = 0; i < n_threads; ++i) {
        threads.emplace_back([&rs]() {
            rs.add_storage(1);
        });
    }
    for (auto&& t : threads) {
        t.join();
    }
    EXPECT_EQ(1, mgr.find_entry(1)->ref_transaction_count());
}

TEST_F(storage_reference_scope_test, concurrent_add_storage_different_entries) {
    // each thread references its own distinct storage; final count per storage must be 1
    std::size_t const n_threads = 8;
    storage_manager mgr{};
    for (std::size_t i = 1; i <= n_threads; ++i) {
        ASSERT_TRUE(mgr.add_entry(static_cast<storage_entry>(i), "T" + std::to_string(i)));
    }

    reference_scope rs{mgr};
    std::vector<std::thread> threads{};
    threads.reserve(n_threads);
    for (std::size_t i = 1; i <= n_threads; ++i) {
        threads.emplace_back([&rs, i]() {
            rs.add_storage(static_cast<storage_entry>(i));
        });
    }
    for (auto&& t : threads) {
        t.join();
    }
    for (std::size_t i = 1; i <= n_threads; ++i) {
        EXPECT_EQ(1, mgr.find_entry(static_cast<storage_entry>(i))->ref_transaction_count());
    }
}

} // namespace jogasaki::storage
