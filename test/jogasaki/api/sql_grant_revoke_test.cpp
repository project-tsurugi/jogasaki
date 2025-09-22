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
#include <gtest/gtest.h>

#include <tateyama/api/server/mock/request_response.h>

#include <jogasaki/auth/action_kind.h>
#include <jogasaki/auth/action_set.h>
#include <jogasaki/auth/authorized_users_action_set.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_req_info.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::auth;

/**
* @brief testcases for SQL GRANT and REVOKE tests
* @details similar to sql_authorization_test, but this test uses GRANT and REVOKE statements
*/
class sql_grant_revoke_test :
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

    void test_set(std::string priv, action_kind kind);
    void test_set_public(std::string priv, action_kind kind);
};

TEST_F(sql_grant_revoke_test, verify_by_action_set) {
    // make sure the logic to verify via action_set members works correctly
    execute_statement("create table t (c0 int primary key)");

    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
    execute_statement("grant select on table t to user1");
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user1"));
    execute_statement("grant insert on table t to user2");
    EXPECT_EQ((action_set{action_kind::insert}), users_actions.find_user_actions("user2"));
    execute_statement("grant delete on table t to user1");
    EXPECT_EQ((action_set{action_kind::select, action_kind::delete_}), users_actions.find_user_actions("user1"));
    EXPECT_TRUE(! public_actions.has_action(action_kind::update));
    EXPECT_EQ((action_set{}), public_actions);
    execute_statement("grant update on table t to public");
    EXPECT_EQ((action_set{action_kind::update}), public_actions);

    execute_statement("revoke select on table t from user1");
    EXPECT_EQ((action_set{action_kind::delete_}), users_actions.find_user_actions("user1"));
    execute_statement("revoke insert on table t from user2");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
    execute_statement("revoke delete on table t from user1");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
    execute_statement("revoke update on table t from public");
    EXPECT_EQ((action_set{}), public_actions);
}

TEST_F(sql_grant_revoke_test, control_privilege_on_create_table) {
    // create table implicitly grants CONTROL to the creator
    auto info = utils::create_req_info("user1");
    execute_statement("create table t (c0 int primary key)", info);
    auto& smgr = *global::storage_manager();
    auto entry_opt = smgr.find_by_name("t");
    ASSERT_TRUE(entry_opt.has_value());
    auto entry = smgr.find_entry(entry_opt.value());
    ASSERT_TRUE(entry);
    auto& users_actions = entry->authorized_actions();
    EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
    execute_statement("revoke all privileges on table t from user1");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
    execute_statement("grant all privileges on table t to user1");
    EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
}

void sql_grant_revoke_test::test_set(std::string priv, action_kind kind) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant "+priv+" on table t to user1");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{kind}), users_actions.find_user_actions("user1"));
    execute_statement("revoke "+priv+" on table t from user1");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
}
void sql_grant_revoke_test::test_set_public(std::string priv, action_kind kind) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant "+priv+" on table t to public");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{kind}), public_actions);
    execute_statement("revoke "+priv+" on table t from public");
    EXPECT_EQ((action_set{}), public_actions);
}

TEST_F(sql_grant_revoke_test, select) {
    test_set("select", action_kind::select);
}

TEST_F(sql_grant_revoke_test, select_by_public_privilege) {
    test_set_public("select", action_kind::select);
}

TEST_F(sql_grant_revoke_test, insert) {
    test_set("insert", action_kind::insert);
}

TEST_F(sql_grant_revoke_test, insert_by_public_privilege) {
    test_set_public("insert", action_kind::insert);
}

TEST_F(sql_grant_revoke_test, update) {
    test_set("update", action_kind::update);
}

TEST_F(sql_grant_revoke_test, update_by_public_privilege) {
    test_set_public("update", action_kind::update);
}

TEST_F(sql_grant_revoke_test, delete) {
    test_set("delete", action_kind::delete_);
}

TEST_F(sql_grant_revoke_test, delete_by_public_privilege) {
    test_set_public("delete", action_kind::delete_);
}

TEST_F(sql_grant_revoke_test, multiple_users) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to user1, user2");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user1"));
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user2"));
    execute_statement("revoke select on table t from user1, user2");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
}

TEST_F(sql_grant_revoke_test, multiple_privileges) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select, insert on table t to user1");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select, action_kind::insert}), users_actions.find_user_actions("user1"));
    execute_statement("revoke select, insert on table t from user1");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
}

TEST_F(sql_grant_revoke_test, multiple_tables) {
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("create table t1 (c0 int primary key)");
    execute_statement("grant select on table t0, t1 to user1");
    auto [users_actions0, public_actions0] = actions("t0");
    auto [users_actions1, public_actions1] = actions("t1");
    EXPECT_EQ((action_set{action_kind::select}), users_actions0.find_user_actions("user1"));
    EXPECT_EQ((action_set{action_kind::select}), users_actions1.find_user_actions("user1"));
    execute_statement("revoke select on table t0, t1 from user1");
    EXPECT_EQ((action_set{}), users_actions0.find_user_actions("user1"));
    EXPECT_EQ((action_set{}), users_actions1.find_user_actions("user1"));
}

TEST_F(sql_grant_revoke_test, multiple_users_tables_privileges) {
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("create table t1 (c0 int primary key)");
    execute_statement("grant select,insert on table t0,t1 to user1, user2");
    auto [users_actions0, public_actions0] = actions("t0");
    auto [users_actions1, public_actions1] = actions("t1");
    EXPECT_EQ((action_set{action_kind::select, action_kind::insert}), users_actions0.find_user_actions("user1"));
    EXPECT_EQ((action_set{action_kind::select, action_kind::insert}), users_actions1.find_user_actions("user1"));
    EXPECT_EQ((action_set{action_kind::select, action_kind::insert}), users_actions0.find_user_actions("user2"));
    EXPECT_EQ((action_set{action_kind::select, action_kind::insert}), users_actions1.find_user_actions("user2"));

    execute_statement("revoke select,insert on table t0,t1 from user1, user2");
    EXPECT_EQ((action_set{}), users_actions0.find_user_actions("user1"));
    EXPECT_EQ((action_set{}), users_actions1.find_user_actions("user1"));
    EXPECT_EQ((action_set{}), users_actions0.find_user_actions("user2"));
    EXPECT_EQ((action_set{}), users_actions1.find_user_actions("user2"));
}

TEST_F(sql_grant_revoke_test, public_and_user) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to user1, public");

    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user1"));
    EXPECT_EQ((action_set{action_kind::select}), public_actions);

    execute_statement("revoke select on table t from user1, public");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
    EXPECT_EQ((action_set{}), public_actions);
}

TEST_F(sql_grant_revoke_test, public_and_user_revoked_separately) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to user1, public");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user1"));
    EXPECT_EQ((action_set{action_kind::select}), public_actions);
    execute_statement("revoke select on table t from public");
    EXPECT_EQ((action_set{}), public_actions);
    execute_statement("revoke select on table t from user1");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
}

TEST_F(sql_grant_revoke_test, insufficient_privilege) {
    // grant/revoke fails due to lack of privileges
    execute_statement("create table t (c0 int primary key)");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("grant select on table t to public", info, error_code::permission_error);
    test_stmt_err("revoke select on table t from public", info, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, grant_revoke_by_control) {
    // grant/revoke allowed by control since it contains ALTER privilege
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant all privileges on table t to user1");
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("grant all privileges on table t to user2", info1);
    execute_statement("grant select on table t to user3", info1);

    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user2"));
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user3"));

    execute_statement("revoke all privileges on table t from user2", info1);
    execute_statement("revoke select on table t from user3", info1);
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user3"));
}

TEST_F(sql_grant_revoke_test, grant_revoke_by_public_control) {
    // similar to grant_revoke_by_public_control, but with public privilege
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant all privileges on table t to public");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::control}), public_actions);

    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("grant all privileges on table t to user2", info1);
    execute_statement("grant select on table t to user3", info1);

    EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user2"));
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user3"));

    execute_statement("revoke all privileges on table t from user2", info1);
    execute_statement("revoke select on table t from user3", info1);

    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user3"));
}

TEST_F(sql_grant_revoke_test, revoke_self) {
    // revoke allowed by control and it revoke itself
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant all privileges on table t to user1");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("revoke all privileges on table t from user1", info1);
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
}

TEST_F(sql_grant_revoke_test, revoke_all) {
    // revoke all privileges remove not only control but also any other privileges
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select, insert, update, delete on table t to user1");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select, action_kind::insert, action_kind::update, action_kind::delete_}), users_actions.find_user_actions("user1"));
    execute_statement("revoke all privileges on table t from user1");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
}

TEST_F(sql_grant_revoke_test, revoke_all_public) {
    // revoke all privileges remove not only control but also any other privileges
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select, insert, update, delete on table t to public");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select, action_kind::insert, action_kind::update, action_kind::delete_}), public_actions);
    execute_statement("revoke all privileges on table t from public");
    EXPECT_EQ((action_set{}), public_actions);
}

TEST_F(sql_grant_revoke_test, missing_table) {
    test_stmt_err("grant select on table t to user1", error_code::symbol_analyze_exception);
    test_stmt_err("revoke select on table t from user1", error_code::symbol_analyze_exception);
}

TEST_F(sql_grant_revoke_test, revoke_empty_privileges) {
    // verify no error with revoke if there are no privileges
    execute_statement("create table t (c0 int primary key)");
    execute_statement("revoke select, insert on table t from user1, user2");
}

TEST_F(sql_grant_revoke_test, grant_duplicate_privileges) {
    // verify no error in granting same privilege mutiple times
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select,select,select on table t to user1");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user1"));
    execute_statement("revoke select,select,select on table t from user1");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
}

TEST_F(sql_grant_revoke_test, grant_duplicate_users) {
    // verify no error in granting to same user multiple times
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to user1,user1");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user1"));
    execute_statement("revoke select on table t from user1,user1");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
}

TEST_F(sql_grant_revoke_test, grant_duplicate_tables) {
    // verify no error in granting privileges on the same table multiple times
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t, t to user1");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user1"));
    execute_statement("revoke select on table t, t from user1");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
}

TEST_F(sql_grant_revoke_test, grant_many_duplicates) {
    // verify no error in granting with many duplicates
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select,select,select on table t, t, t to user1, user1, user1, public, public");
    auto [users_actions, public_actions] = actions("t");
    EXPECT_EQ((action_set{action_kind::select}), users_actions.find_user_actions("user1"));
    EXPECT_EQ((action_set{action_kind::select}), public_actions);
    execute_statement("revoke select,select,select on table t, t, t from user1, user1, user1, public, public");
    EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
    EXPECT_EQ((action_set{}), public_actions);
}

TEST_F(sql_grant_revoke_test, storage_lock_released_after_grant_fails) {
    // verify grant on t1 fails due to missing table, and the lock on t0 is released properly
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("insert into t0 values (1)");
    execute_statement("create table t1 (c0 int primary key)");

    std::string sql = "grant select on table t0, t1 to public";
    api::statement_handle handle{};

    std::unordered_map<std::string, api::field_type_kind> variables{};
    ASSERT_EQ(status::ok, db_->prepare(sql, variables, handle));
    ASSERT_TRUE(handle);
    execute_statement("drop table t1");
    auto tx0 = utils::create_transaction(*db_);
    execute_statement(handle, *tx0, status::err_not_found);

    // tx0 must have been aborted
    test_stmt_err("select * from t0", *tx0, error_code::inactive_transaction_exception);
    execute_statement("select * from t0"); // to verify there is no lock left on t0
}

TEST_F(sql_grant_revoke_test, grant_revoke_current_user) {
    // verify use of CURRENT_USER
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant all privileges on table t to user1");
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
    }
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);

    // as user1 already has control, so this is actually no-op
    execute_statement("grant all privileges on table t to current_user", info1);
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
    }
    // as user1 already has control, so this is actually no-op
    execute_statement("grant select on table t to current_user", info1);
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
    }

    // current_user is available only when authentication is enabled
    test_stmt_err("grant all privileges on table t to current_user", error_code::value_evaluation_exception);
    test_stmt_err("revoke all privileges on table t from current_user", error_code::value_evaluation_exception);

    execute_statement("revoke select on table t from current_user", info1);  // revoking select is no-op as user1 has control
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
    }

    execute_statement("revoke all privileges on table t from current_user", info1);
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
    }

    // now user1 has no privilege, so this fails
    test_stmt_err("revoke select on table t from current_user", info1, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, grant_revoke_all_users) {
    // verify use of `*` (meaning all users except current user)
    execute_statement("create table t (c0 int primary key)");

    // grant ... to * is compile error (never supported)
    test_stmt_err("grant all privileges on table t to *", error_code::syntax_exception);

    execute_statement("grant all privileges on table t to public");
    execute_statement("grant all privileges on table t to user1");
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("grant all privileges on table t to user2", info1);
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{action_kind::control}), public_actions);
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user2"));
    }

    // revoke from * is supported only for "all privileges". Revoking any other single privilege is not supported
    test_stmt_err("revoke select on table t from *", info1, error_code::unsupported_runtime_feature_exception);

    // `*` is available only when authentication is enabled
    test_stmt_err("revoke all privileges on table t from *", error_code::value_evaluation_exception);

    execute_statement("revoke all privileges on table t from *", info1);
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
    }
    execute_statement("revoke all privileges on table t from current_user", info1);
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
    }
}

TEST_F(sql_grant_revoke_test, grant_revoke_all_and_current_users) {
    // verify use of `*` together with "CURRENT_USER"
    execute_statement("create table t (c0 int primary key)");

    execute_statement("grant all privileges on table t to public");
    execute_statement("grant all privileges on table t to user1");
    execute_statement("grant select, insert on table t to user2");
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{action_kind::control}), public_actions);
        EXPECT_EQ((action_set{action_kind::control}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{action_kind::select, action_kind::insert}), users_actions.find_user_actions("user2"));
    }

    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("revoke all privileges on table t from *, user1", info1);
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
    }
    execute_statement("grant all privileges on table t to public");
    execute_statement("grant all privileges on table t to user1");
    execute_statement("grant select, insert on table t to user2");
    execute_statement("revoke all privileges on table t from current_user, *", info1);
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
    }
    execute_statement("grant all privileges on table t to public");
    execute_statement("grant all privileges on table t to user1");
    execute_statement("grant select, insert on table t to user2");
    execute_statement("revoke all privileges on table t from current_user, *, *, current_user", info1);
    {
        auto [users_actions, public_actions] = actions("t");
        EXPECT_EQ((action_set{}), public_actions);
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user1"));
        EXPECT_EQ((action_set{}), users_actions.find_user_actions("user2"));
    }
}


}  // namespace jogasaki::testing
