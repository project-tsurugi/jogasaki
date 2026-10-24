/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <functional>
#include <future>
#include <iostream>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/executor/function/incremental/builtin_functions.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/relation_io_map.h>
#include <jogasaki/executor/process/step.h>
#include <jogasaki/executor/writer_count_calculator.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/memory/page_pool.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/plan/compiler_context.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/test_utils.h>
#include <jogasaki/utils/field_types.h>

namespace jogasaki::storage {

using namespace std::string_literals;
using namespace std::string_view_literals;
using namespace meta;
using namespace takatori::util;

namespace type      = ::takatori::type;
namespace value     = ::takatori::value;
namespace scalar    = ::takatori::scalar;
namespace relation  = ::takatori::relation;
namespace statement = ::takatori::statement;

using namespace testing;

using namespace ::yugawara::variable;
using namespace ::yugawara;

using namespace jogasaki::auth;

/**
 * @brief test storage manager
 */
class storage_manager_test : public ::testing::Test {

};

TEST_F(storage_manager_test, add) {
    // add new entries
    storage_manager mgr{};
    EXPECT_EQ(0, mgr.size());
    ASSERT_TRUE(mgr.add_entry(1, "T1"));
    EXPECT_EQ(1, mgr.size());
    ASSERT_TRUE(mgr.add_entry(2, "T2"));
    EXPECT_EQ(2, mgr.size());
    ASSERT_TRUE(! mgr.add_entry(1, "T1"));
    ASSERT_TRUE(! mgr.add_entry(2, "T2"));
}

TEST_F(storage_manager_test, find) {
    // find entries
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));
    ASSERT_TRUE(mgr.add_entry(2, "T2"));
    ASSERT_TRUE(mgr.find_entry(1));
    ASSERT_TRUE(mgr.find_entry(2));
    ASSERT_TRUE(! mgr.find_entry(3));
}

TEST_F(storage_manager_test, find_by_name) {
    // find storage key by name
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));
    ASSERT_TRUE(mgr.add_entry(2, "T2"));
    ASSERT_EQ(1, mgr.find_by_name("T1"));
    ASSERT_EQ(2, mgr.find_by_name("T2"));
    ASSERT_TRUE(! mgr.find_by_name("T3"));
}

TEST_F(storage_manager_test, get_storage_key) {
    // verify specifying storage key
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1", "K1"));
    ASSERT_TRUE(mgr.add_entry(2, "T2", "K2"));
    ASSERT_TRUE(mgr.add_entry(3, "T3"));
    ASSERT_EQ("K1", mgr.get_storage_key("T1"));
    ASSERT_EQ("K2", mgr.get_storage_key("T2"));
    ASSERT_EQ("T3", mgr.get_storage_key("T3"));
    ASSERT_TRUE(! mgr.get_storage_key("T4"));
}

TEST_F(storage_manager_test, get_storage_name) {
    // verify get_storage_name
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1", "K1"));
    ASSERT_TRUE(mgr.add_entry(2, "T2", "K2"));
    ASSERT_TRUE(mgr.add_entry(3, "T3"));
    ASSERT_EQ("T1", mgr.get_index_name("K1"));
    ASSERT_EQ("T2", mgr.get_index_name("K2"));
    ASSERT_EQ("T3", mgr.get_index_name("T3"));
    ASSERT_TRUE(! mgr.get_index_name("T4"));
}

TEST_F(storage_manager_test, remove) {
    // remove entries
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));
    ASSERT_TRUE(mgr.add_entry(2, "T2"));
    ASSERT_TRUE(mgr.add_entry(10, "T10", "K10"));
    ASSERT_TRUE(mgr.find_entry(1));
    ASSERT_TRUE(mgr.find_entry(10));
    EXPECT_EQ(3, mgr.size());
    EXPECT_TRUE(mgr.is_current_entry(1));
    ASSERT_TRUE(mgr.remove_entry(1));
    EXPECT_EQ(2, mgr.size());
    ASSERT_TRUE(! mgr.find_entry(1));
    EXPECT_FALSE(mgr.is_current_entry(1));
    ASSERT_TRUE(! mgr.get_storage_key("T1"));
    ASSERT_TRUE(! mgr.get_index_name("T1"));
    ASSERT_TRUE(! mgr.remove_entry(3));
    EXPECT_EQ(2, mgr.size());
    ASSERT_TRUE(mgr.remove_entry(10));
    ASSERT_TRUE(! mgr.find_entry(10));
    ASSERT_TRUE(! mgr.get_storage_key("T10"));
    ASSERT_TRUE(! mgr.get_index_name("K10"));
}

TEST_F(storage_manager_test, acquire_lock) {
    // acquire shared lock
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    mgr.add_entry(2, "T2");
    storage_list stgs{1, 2};
    {
        auto sl = mgr.create_shared_lock(stgs);
        ASSERT_TRUE(sl);
        EXPECT_EQ(storage_list_view{stgs}, sl->storage());
    }
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(ul);
        EXPECT_EQ(storage_list_view{}, ul->storage());
        ASSERT_TRUE(mgr.add_locked_storages(stgs, *ul));
        EXPECT_EQ(storage_list_view{stgs}, ul->storage());
    }
    {
        // check releasing part of the unique lock
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(mgr.add_locked_storages(stgs, *ul));
        EXPECT_EQ(storage_list_view{stgs}, ul->storage());
        storage_list stg1{1};
        storage_list stg2{2};
        mgr.remove_locked_storages(stg1, *ul);
        EXPECT_EQ(storage_list_view{stg2}, ul->storage());
        mgr.remove_locked_storages(stg2, *ul);
        EXPECT_EQ(storage_list_view{}, ul->storage());
    }
}

TEST_F(storage_manager_test, acquire_lock_multiple_times) {
    // acquire shared lock
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    mgr.add_entry(2, "T2");
    storage_list stgs{1, 2};
    {
        auto sl0 = mgr.create_shared_lock(stgs);
        ASSERT_TRUE(sl0);
        auto sl1 = mgr.create_shared_lock(stgs);
        ASSERT_TRUE(sl1);
        EXPECT_EQ(storage_list_view{stgs}, sl0->storage());
        EXPECT_EQ(storage_list_view{stgs}, sl1->storage());
    }
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(ul);
        storage_list stg1{1};
        ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
        EXPECT_EQ(storage_list_view{stg1}, ul->storage());
        storage_list stg2{2};
        ASSERT_TRUE(mgr.add_locked_storages(stg2, *ul));
        EXPECT_EQ(storage_list_view{stgs}, ul->storage());
        ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));  // adding existing storage again
        EXPECT_EQ(storage_list_view{stgs}, ul->storage());
    }
}

TEST_F(storage_manager_test, dml) {
    // verify lock operations made from dml-only transaction
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    mgr.add_entry(2, "T2");
    storage_list stgs{1, 2};
    {
        auto sl = mgr.create_shared_lock(stgs);
        ASSERT_TRUE(sl);
        // do DML operations
        // SELECT * FROM T1, T2
    }
    {
        auto sl = mgr.create_shared_lock(stgs);
        ASSERT_TRUE(sl);
        // do DML operations
        // SELECT * FROM T1, T2
    }
}

TEST_F(storage_manager_test, dml_blocked_by_ddl) {
    // verify acquiring shared lock is blocked by unique lock held by ddl transaction
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    mgr.add_entry(2, "T2");
    storage_list stg1{1};
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(ul);
        ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
        {
            auto sl = mgr.create_shared_lock(stg1);
            ASSERT_TRUE(! sl);
        }
    }
    {
        auto sl = mgr.create_shared_lock(stg1);
        ASSERT_TRUE(sl);
    }
}

TEST_F(storage_manager_test, ddl) {
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    mgr.add_entry(2, "T2");
    storage_list stg1{1};
    storage_list stg2{2};
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(ul);
        ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
        // DROP TABLE T1
        mgr.remove_locked_storages(stg1, *ul);

        ASSERT_TRUE(mgr.add_locked_storages(stg2, *ul));
        // DROP TABLE T2
        mgr.remove_locked_storages(stg2, *ul);
    }
}

TEST_F(storage_manager_test, ddl_blocked_by_dml) {
    // verify acquiring unique lock is blocked by shared lock held by dml transaction
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    mgr.add_entry(2, "T2");
    storage_list stg1{1};
    storage_list stgs{1, 2};

    auto ul = mgr.create_unique_lock();
    ASSERT_TRUE(ul);
    {
        auto sl = mgr.create_shared_lock(stgs);
        ASSERT_TRUE(sl);
        ASSERT_TRUE(! mgr.add_locked_storages(stg1, *ul));
    }
    ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
}

TEST_F(storage_manager_test, ddl_and_dml) {
    // verify mixed ddl and dml operations in one transaction
    storage_manager mgr{};
    storage_list stg1{1};
    storage_list stg2{2};
    storage_list stgs{1, 2};
    {
        // unique lock held by transaction context
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(ul);

        ASSERT_TRUE(mgr.add_entry(1, "T1"));
        // CREATE TABLE T1
        ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));

        ASSERT_TRUE(mgr.add_entry(2, "T2"));
        // CREATE TABLE T2
        ASSERT_TRUE(mgr.add_locked_storages(stg2, *ul));
        {
            // shared lock held by request context
            auto sl = mgr.create_shared_lock(stgs, ul.get());
            ASSERT_TRUE(sl);
            // do DML operations
            // SELECT * FROM T1, T2
        }
        {
            auto sl = mgr.create_shared_lock(stgs, ul.get());
            ASSERT_TRUE(sl);
            // do DML operations
            // SELECT * FROM T1, T2
        }
        ASSERT_TRUE(mgr.add_locked_storages(stg2, *ul));
        // DROP TABLE T2
        ASSERT_TRUE(mgr.remove_entry(2));
        ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
        // DROP TABLE T1
        ASSERT_TRUE(mgr.remove_entry(1));
    }
}

TEST_F(storage_manager_test, add_entry_with_lock) {
    // verify the entry added with unique lock is locked before it becomes visible by name
    storage_manager mgr{};
    storage_list stg1{1};
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(mgr.add_entry(1, "T1", std::nullopt, true, std::nullopt, std::nullopt, ul.get()));
        EXPECT_EQ(1, ul->storage().size());
        EXPECT_TRUE(ul->storage().contains(1));
        auto e = mgr.find_by_name("T1");
        ASSERT_TRUE(e);
        EXPECT_EQ(1, e.value());
        {
            auto sl = mgr.create_shared_lock(stg1);
            ASSERT_TRUE(! sl);
        }
        {
            // shared lock in the same transaction is allowed
            auto sl = mgr.create_shared_lock(stg1, ul.get());
            ASSERT_TRUE(sl);
        }
    }
    {
        auto sl = mgr.create_shared_lock(stg1);
        ASSERT_TRUE(sl);
    }
}

TEST_F(storage_manager_test, add_entry_with_lock_holding_other_entries) {
    // verify the entry added with unique lock is merged into the existing lock
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    storage_list stg1{1};
    storage_list stg2{2};
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
        ASSERT_TRUE(mgr.add_entry(2, "T2", std::nullopt, true, std::nullopt, std::nullopt, ul.get()));
        EXPECT_TRUE(ul->storage().contains(1));
        EXPECT_TRUE(ul->storage().contains(2));
        ASSERT_TRUE(! mgr.create_shared_lock(stg1));
        ASSERT_TRUE(! mgr.create_shared_lock(stg2));
    }
    ASSERT_TRUE(mgr.create_shared_lock(stg1));
    ASSERT_TRUE(mgr.create_shared_lock(stg2));
}

TEST_F(storage_manager_test, lock_delete_reserved_entry) {
    // verify delete-reserved entry (e.g. dropped or truncated) cannot be locked even after DDL lock is released
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    storage_list stg1{1};
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
        EXPECT_TRUE(mgr.is_current_entry(1));
        ASSERT_TRUE(mgr.reserve_delete_entry(1));
        EXPECT_FALSE(mgr.is_current_entry(1));
    }
    ASSERT_TRUE(mgr.find_entry(1));
    ASSERT_TRUE(! mgr.create_shared_lock(stg1));
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(! mgr.add_locked_storages(stg1, *ul));
    }
    // failed attempts should not leave the entry locked
    auto c = mgr.find_entry(1);
    ASSERT_TRUE(c);
    EXPECT_TRUE(c->can_lock());
}

TEST_F(storage_manager_test, lock_delete_reserved_entry_held_by_parent) {
    // verify delete-reserved entry cannot be locked even if the parent lock (i.e. the same transaction) holds it
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    storage_list stg1{1};
    auto ul = mgr.create_unique_lock();
    ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
    ASSERT_TRUE(mgr.create_shared_lock(stg1, ul.get()));
    ASSERT_TRUE(mgr.reserve_delete_entry(1));
    ASSERT_TRUE(! mgr.create_shared_lock(stg1, ul.get()));
    ASSERT_TRUE(! mgr.add_locked_storages(stg1, *ul));
    EXPECT_TRUE(ul->storage().contains(1));
}

// run writer and reader concurrently, and abort if they don't finish (deadlocked threads can't be joined)
static void run_concurrently_or_abort(std::function<void()> writer, std::function<void()> reader) {
    std::atomic_bool stop{false};
    auto w = std::async(std::launch::async, [&]() {
        writer();
        stop = true;
    });
    auto r = std::async(std::launch::async, [&]() {
        while(! stop) {
            reader();
        }
    });
    for(auto* f : {&w, &r}) {
        if(f->wait_for(std::chrono::seconds{30}) != std::future_status::ready) {
            std::cerr << "deadlock detected" << std::endl;
            std::abort();
        }
    }
}

TEST_F(storage_manager_test, reserve_delete_entry_while_getting_storage_key) {
    // regression test: reserve_delete_entry() and get_storage_key() for the same name used to deadlock
    storage_manager mgr{};
    run_concurrently_or_abort(
        [&]() {
            for(storage_entry e = 1; e <= 1000; ++e) {
                ASSERT_TRUE(mgr.add_entry(e, "T1"));
                ASSERT_TRUE(mgr.reserve_delete_entry(e));
            }
        },
        [&]() {
            (void) mgr.get_storage_key("T1");
        }
    );
}

TEST_F(storage_manager_test, remove_entry_while_getting_index_name) {
    // regression test: remove_entry() and get_index_name() for the same storage key used to deadlock
    storage_manager mgr{};
    run_concurrently_or_abort(
        [&]() {
            for(storage_entry e = 1; e <= 1000; ++e) {
                ASSERT_TRUE(mgr.add_entry(e, "T1", "key"));
                ASSERT_TRUE(mgr.remove_entry(e));
            }
        },
        [&]() {
            (void) mgr.get_index_name("key");
        }
    );
}

TEST_F(storage_manager_test, add_entry_keeps_existing_name) {
    // verify add_entry() doesn't change the entry the name refers to if the name is already registered
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));
    ASSERT_TRUE(mgr.add_entry(2, "T1"));
    EXPECT_EQ(1, mgr.find_by_name("T1"));
    // the entry whose name is not registered is not current, and cannot be locked
    EXPECT_TRUE(mgr.is_current_entry(1));
    EXPECT_FALSE(mgr.is_current_entry(2));
    storage_list stg1{1};
    storage_list stg2{2};
    EXPECT_TRUE(mgr.create_shared_lock(stg1));
    EXPECT_FALSE(mgr.create_shared_lock(stg2));
}

TEST_F(storage_manager_test, add_entry_without_name) {
    // verify the entry added without name (delete-reserved, e.g. on recovery) is not current, and cannot be locked
    storage_manager mgr{};
    ASSERT_TRUE(mgr.add_entry(1, std::nullopt, std::nullopt, true, std::nullopt, "T1"));
    EXPECT_FALSE(mgr.is_current_entry(1));
    storage_list stg1{1};
    EXPECT_FALSE(mgr.create_shared_lock(stg1));
}

TEST_F(storage_manager_test, replace_entry) {
    // verify replace_entry() re-points the name from the replaced entry to the new one
    storage_manager mgr{};
    storage_list stg1{1};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));
    ASSERT_TRUE(mgr.add_entry(2, "I1", std::nullopt, false, 1));  // secondary index of T1
    auto ul = mgr.create_unique_lock();
    ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
    ASSERT_TRUE(mgr.replace_entry(3, 1, "T1", std::nullopt, true, std::nullopt, *ul));
    ASSERT_TRUE(mgr.replace_entry(4, 2, "I1", std::nullopt, false, 3, *ul));
    EXPECT_EQ(3, mgr.find_by_name("T1"));
    EXPECT_EQ(4, mgr.find_by_name("I1"));
    // new primary entry is locked by the lock, but secondary is not
    EXPECT_TRUE(ul->storage().contains(3));
    EXPECT_FALSE(ul->storage().contains(4));
    // the replaced entries are left as they are, but no longer current
    EXPECT_TRUE(mgr.find_entry(1));
    EXPECT_TRUE(mgr.find_entry(2));
    EXPECT_FALSE(mgr.is_current_entry(1));
    EXPECT_FALSE(mgr.is_current_entry(2));
    EXPECT_TRUE(mgr.is_current_entry(3));
    EXPECT_TRUE(mgr.is_current_entry(4));
}

TEST_F(storage_manager_test, replace_entry_preconditions) {
    // verify replace_entry() fails without side effect if the preconditions are not satisfied
    storage_manager mgr{};
    storage_list stg1{1};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));
    ASSERT_TRUE(mgr.add_entry(2, "I1", std::nullopt, false, 1));  // secondary index of T1
    ASSERT_TRUE(mgr.add_entry(5, "T2"));
    {
        // replaced entry (or its primary) is not locked by the lock
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(! mgr.replace_entry(3, 1, "T1", std::nullopt, true, std::nullopt, *ul));
        ASSERT_TRUE(! mgr.replace_entry(4, 2, "I1", std::nullopt, false, 3, *ul));
    }
    {
        // the name doesn't refer to the replaced entry
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
        ASSERT_TRUE(! mgr.replace_entry(3, 1, "T2", std::nullopt, true, std::nullopt, *ul));
        EXPECT_EQ(1, ul->storage().size());
    }
    EXPECT_EQ(1, mgr.find_by_name("T1"));
    EXPECT_EQ(2, mgr.find_by_name("I1"));
    EXPECT_EQ(5, mgr.find_by_name("T2"));
    EXPECT_FALSE(mgr.find_entry(3));
    EXPECT_FALSE(mgr.find_entry(4));
    EXPECT_TRUE(mgr.is_current_entry(1));
    EXPECT_TRUE(mgr.is_current_entry(2));
    EXPECT_TRUE(mgr.is_current_entry(5));
}

TEST_F(storage_manager_test, reserve_delete_entry_keeps_name_referring_to_other_entry) {
    // verify reserve_delete_entry() doesn't erase the name if it has been re-pointed to another entry
    storage_manager mgr{};
    storage_list stg1{1};
    ASSERT_TRUE(mgr.add_entry(1, "T1"));
    auto ul = mgr.create_unique_lock();
    ASSERT_TRUE(mgr.add_locked_storages(stg1, *ul));
    ASSERT_TRUE(mgr.replace_entry(2, 1, "T1", std::nullopt, true, std::nullopt, *ul));
    ASSERT_TRUE(mgr.reserve_delete_entry(1));
    EXPECT_EQ(2, mgr.find_by_name("T1"));
    EXPECT_FALSE(mgr.is_current_entry(1));
    EXPECT_TRUE(mgr.is_current_entry(2));
    ASSERT_TRUE(mgr.reserve_delete_entry(2));
    EXPECT_FALSE(mgr.find_by_name("T1"));
    EXPECT_FALSE(mgr.is_current_entry(2));
}

TEST_F(storage_manager_test, lock_entry_whose_name_is_re_pointed) {
    // verify the entry whose name has been re-pointed to another entry cannot be locked even if it's not
    // delete-reserved (e.g. TRUNCATE TABLE failed after creating the new entry)
    storage_manager mgr{};
    storage_list stg1{1};
    storage_list stg2{2};
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(mgr.add_entry(1, "T1", std::nullopt, true, std::nullopt, std::nullopt, ul.get()));
        ASSERT_TRUE(mgr.replace_entry(2, 1, "T1", std::nullopt, true, std::nullopt, *ul));
        // same transaction
        ASSERT_TRUE(! mgr.create_shared_lock(stg1, ul.get()));
        ASSERT_TRUE(mgr.create_shared_lock(stg2, ul.get()));
    }
    // other transaction after the lock is released
    ASSERT_TRUE(! mgr.create_shared_lock(stg1));
    ASSERT_TRUE(mgr.create_shared_lock(stg2));
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(! mgr.add_locked_storages(stg1, *ul));
    }
    // failed attempts should not leave the entry locked
    auto c = mgr.find_entry(1);
    ASSERT_TRUE(c);
    EXPECT_TRUE(c->can_lock());
}

TEST_F(storage_manager_test, err_try_to_lock_non_exiting_storage) {
    // erroneous case locking non-exiting storage
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    storage_list stg2{2};
    {
        auto sl = mgr.create_shared_lock(stg2);
        ASSERT_TRUE(! sl);
    }
    {
        auto ul = mgr.create_unique_lock();
        auto sl = mgr.add_locked_storages(stg2, *ul);
        ASSERT_TRUE(! sl);
    }
}

TEST_F(storage_manager_test, err_locked_entry_suddenly_disappears) {
    // erroneous case shared-locked entry removed while locked - check no error or crash happens
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    storage_list stg{1};
    {
        auto sl = mgr.create_shared_lock(stg);
        ASSERT_TRUE(sl);
        ASSERT_TRUE(mgr.remove_entry(1));
    }
    mgr.add_entry(1, "T1");
    {
        auto ul = mgr.create_unique_lock();
        ASSERT_TRUE(mgr.add_locked_storages(stg, *ul));
        ASSERT_TRUE(mgr.remove_entry(1));
    }
}

TEST_F(storage_manager_test, allows_user_actions) {
    // verify allows_user_actions that checks users existence and privileges
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    auto stg = mgr.find_entry(1);
    ASSERT_TRUE(stg);

    stg->authorized_actions().add_user_actions("user5", action_set{action_kind::select, action_kind::insert});

    EXPECT_TRUE(stg->allows_user_actions("user5",action_set{action_kind::select}));
    EXPECT_TRUE(stg->allows_user_actions("user5",action_set{action_kind::select, action_kind::insert}));
    EXPECT_TRUE(! stg->allows_user_actions("user5",action_set{action_kind::select, action_kind::insert, action_kind::control}));
}

TEST_F(storage_manager_test, allows_user_actions_by_users_and_public_privs) {
    // verify allows_user_actions that checks both users and public privileges
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    auto stg = mgr.find_entry(1);
    ASSERT_TRUE(stg);

    stg->authorized_actions().add_user_actions("user5", action_set{action_kind::select});
    stg->public_actions().add_action(action_kind::update);

    EXPECT_TRUE(stg->allows_user_actions("user5",action_set{action_kind::select}));
    EXPECT_TRUE(stg->allows_user_actions("user5",action_set{action_kind::select, action_kind::update}));
    EXPECT_TRUE(! stg->allows_user_actions("user5",action_set{action_kind::select, action_kind::update, action_kind::control}));
}

TEST_F(storage_manager_test, allows_user_actions_find_no_user) {
    // verify allows_user_actions returns false if user not found
    storage_manager mgr{};
    mgr.add_entry(1, "T1");
    auto stg = mgr.find_entry(1);
    ASSERT_TRUE(stg);
    stg->public_actions().add_action(action_kind::update);

    EXPECT_TRUE(! stg->allows_user_actions("dummy",action_set{action_kind::select}));
    EXPECT_TRUE(stg->allows_user_actions("dummy",action_set{action_kind::update}));  // public privilege applies even for non-existing user
    EXPECT_TRUE(! stg->allows_user_actions("dummy",action_set{action_kind::select, action_kind::update}));
    EXPECT_TRUE(! stg->allows_user_actions("dummy",action_set{action_kind::select, action_kind::update, action_kind::control}));
}

} // namespace jogasaki::plan
