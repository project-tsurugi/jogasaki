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

#include <memory>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <tateyama/api/server/mock/request_response.h>

#include <jogasaki/auth/action_kind.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_req_info.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;

using namespace jogasaki::auth;

class recovery_authorization_test :
    public ::testing::Test,
    public api_test_base {

public:
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

    std::pair<auth::authorized_users_action_set&, auth::action_set&> actions(std::string_view storage) {
        using ret_type = decltype( actions(std::declval<std::string_view>()) );

        static auth::authorized_users_action_set empty_users_actions{};
        static auth::action_set empty_actions{};

        auto& smgr = *global::storage_manager();
        auto entry_opt = smgr.find_by_name(storage);
        if(! entry_opt.has_value()) {
            ADD_FAILURE();
            return ret_type{empty_users_actions, empty_actions};
        }
        auto entry = smgr.find_entry(entry_opt.value());
        if(! entry) {
            ADD_FAILURE();
            return ret_type{empty_users_actions, empty_actions};
        }
        auto& users_actions = entry->authorized_actions();
        auto& public_actions = entry->public_actions();
        return ret_type{entry->authorized_actions(), entry->public_actions()};
    }
};

TEST_F(recovery_authorization_test, owner_control_persists_after_recovery) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    // control priv. given to owner persists
    auto info = utils::create_req_info("user1");
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)", info);

    // shutdown and restart database
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
}

TEST_F(recovery_authorization_test, multi_privs_persist_after_recovery) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    // granted select/insert/update/delete privs. persist
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");
    execute_statement("grant select, insert, update, delete on table t to user1, user2");
    auto info = utils::create_req_info("user1");

    // shutdown and restart database
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{action_kind::select, action_kind::insert, action_kind::update, action_kind::delete_}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{action_kind::select, action_kind::insert, action_kind::update, action_kind::delete_}), users_actions.find_user_actions("user2"));

        execute_statement("revoke select, insert, update, delete on table t from user1");
        execute_statement("grant select, insert, update, delete on table t to public");
    }
    // shutdown and restart database
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{action_kind::select, action_kind::insert, action_kind::update, action_kind::delete_}), users_actions.find_user_actions("user2"));
        EXPECT_EQ((action_set{action_kind::select, action_kind::insert, action_kind::update, action_kind::delete_}), public_actions);
    }
}

TEST_F(recovery_authorization_test, granted_control_persists_after_recovery) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    // similar to multi_privs_persist_after_recovery, but use "all privileges" for grant and revoke
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");
    execute_statement("grant all privileges on table t to user1, user2");
    auto info = utils::create_req_info("user1");

    // shutdown and restart database
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user2"));
    }
    execute_statement("revoke all privileges on table t from user1, user2");
    // shutdown and restart database
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
        EXPECT_EQ((action_set{}), public_actions);
    }
}

} // namespace jogasaki::testing
