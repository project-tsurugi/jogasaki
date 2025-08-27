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
};

TEST_F(sql_grant_revoke_test, control_privilege_on_create_table) {
    auto info = utils::create_req_info("user1");
    execute_statement("create table t (c0 int primary key)", info);
    auto& smgr = *global::storage_manager();
    auto entry_opt = smgr.find_by_name("t");
    ASSERT_TRUE(entry_opt.has_value());
    auto entry = smgr.find_entry(entry_opt.value());
    ASSERT_TRUE(entry);
    auto& users_actions = entry->authorized_actions();
    {
        auto actions = users_actions.find_user_actions("user1");
        EXPECT_TRUE(actions.has_action(action_kind::control));
    }
    execute_statement("revoke all privileges on table t from user1");
    // re-define user1 as standard user in order to test auth
    auto standard_user_info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("select * from t", standard_user_info, error_code::permission_error);
    {
        auto actions = users_actions.find_user_actions("user1");
        EXPECT_TRUE(! actions.has_action(action_kind::control));
    }
    execute_statement("grant all privileges on table t to user1");
    {
        auto actions = users_actions.find_user_actions("user1");
        EXPECT_TRUE(actions.has_action(action_kind::control));
    }
}

TEST_F(sql_grant_revoke_test, check_action_set) {
    // directly check the action_set with grant/revoke
    execute_statement("create table t (c0 int primary key)");

    auto& smgr = *global::storage_manager();
    auto entry_opt = smgr.find_by_name("t");
    ASSERT_TRUE(entry_opt.has_value());
    auto entry = smgr.find_entry(entry_opt.value());
    ASSERT_TRUE(entry);
    auto& users_actions = entry->authorized_actions();
    auto& public_actions = entry->public_actions();

    {
        auto actions = users_actions.find_user_actions("user1");
        EXPECT_TRUE(! actions.has_action(action_kind::select));
    }
    execute_statement("grant select on table t to user1");
    {
        auto actions = users_actions.find_user_actions("user1");
        EXPECT_TRUE(actions.has_action(action_kind::select));
    }
    execute_statement("grant insert on table t to user2");
    {
        auto actions = users_actions.find_user_actions("user2");
        EXPECT_TRUE(! actions.has_action(action_kind::select));
        EXPECT_TRUE(actions.has_action(action_kind::insert));
    }
    execute_statement("grant delete on table t to user1");
    {
        auto actions = users_actions.find_user_actions("user1");
        EXPECT_TRUE(actions.has_action(action_kind::select));
        EXPECT_TRUE(! actions.has_action(action_kind::insert));
        EXPECT_TRUE(actions.has_action(action_kind::delete_));
    }
    EXPECT_TRUE(! public_actions.has_action(action_kind::update));
    execute_statement("grant update on table t to public");
    EXPECT_TRUE(public_actions.has_action(action_kind::update));

    execute_statement("revoke select on table t from user1");
    {
        auto actions = users_actions.find_user_actions("user1");
        EXPECT_TRUE(! actions.has_action(action_kind::select));
    }
    execute_statement("revoke insert on table t from user2");
    {
        auto actions = users_actions.find_user_actions("user2");
        EXPECT_TRUE(! actions.has_action(action_kind::select));
        EXPECT_TRUE(! actions.has_action(action_kind::insert));
    }
    execute_statement("revoke delete on table t from user1");
    {
        auto actions = users_actions.find_user_actions("user1");
        EXPECT_TRUE(! actions.has_action(action_kind::select));
        EXPECT_TRUE(! actions.has_action(action_kind::insert));
        EXPECT_TRUE(! actions.has_action(action_kind::delete_));
    }
    execute_statement("revoke update on table t from public");
    EXPECT_TRUE(! public_actions.has_action(action_kind::update));
}

TEST_F(sql_grant_revoke_test, select) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info);
    execute_statement("revoke select on table t from user1");
    test_stmt_err("select * from t", info, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, select_by_public_privilege) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to public");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info);
    execute_statement("revoke select on table t from public");
    test_stmt_err("select * from t", info, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, insert) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant insert on table t to user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("insert into t values (1)", info);
    execute_statement("revoke insert on table t from user1");
    test_stmt_err("insert into t values (2)", info, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, multiple_users) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to user1, user2");
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    auto info2 = utils::create_req_info("user2", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info1);
    execute_statement("select * from t", info2);
    execute_statement("revoke select on table t from user1, user2");
    test_stmt_err("select * from t", info1, error_code::permission_error);
    test_stmt_err("select * from t", info2, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, multiple_privileges) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select, insert on table t to user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("insert into t values (1)", info);
    execute_statement("select * from t", info);
    execute_statement("revoke select, insert on table t from user1");
    test_stmt_err("select * from t", info, error_code::permission_error);
    test_stmt_err("insert into t values (2)", info, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, multiple_tables) {
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("create table t1 (c0 int primary key)");
    execute_statement("grant select on table t0, t1 to user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t0", info);
    execute_statement("select * from t1", info);
    execute_statement("revoke select on table t0, t1 from user1");
    test_stmt_err("select * from t0", info, error_code::permission_error);
    test_stmt_err("select * from t1", info, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, multiple_users_tables_privileges) {
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("create table t1 (c0 int primary key)");
    execute_statement("grant select,insert on table t0,t1 to user1, user2");
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    auto info2 = utils::create_req_info("user2", tateyama::api::server::user_type::standard);
    execute_statement("insert into t0 values (10)", info1);
    execute_statement("insert into t0 values (20)", info2);
    execute_statement("insert into t1 values (10)", info1);
    execute_statement("insert into t1 values (20)", info1);
    execute_statement("select * from t0", info1);
    execute_statement("select * from t1", info1);
    execute_statement("select * from t0", info2);
    execute_statement("select * from t1", info2);
    execute_statement("revoke select,insert on table t0,t1 from user1, user2");
    test_stmt_err("insert into t0 values (100)", info1, error_code::permission_error);
    test_stmt_err("insert into t0 values (200)", info2, error_code::permission_error);
    test_stmt_err("insert into t1 values (100)", info1, error_code::permission_error);
    test_stmt_err("insert into t1 values (200)", info2, error_code::permission_error);
    test_stmt_err("select * from t0", info1, error_code::permission_error);
    test_stmt_err("select * from t1", info1, error_code::permission_error);
    test_stmt_err("select * from t0", info2, error_code::permission_error);
    test_stmt_err("select * from t1", info2, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, public_and_user) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to user1, public");
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    auto info2 = utils::create_req_info("user2", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info1);
    execute_statement("select * from t", info2);
    execute_statement("revoke select on table t from user1, public");
    test_stmt_err("select * from t", info1, error_code::permission_error);
    test_stmt_err("select * from t", info2, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, public_and_user_revoked_separately) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to user1, public");
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    auto info2 = utils::create_req_info("user2", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info1);
    execute_statement("select * from t", info2);
    execute_statement("revoke select on table t from public");
    execute_statement("select * from t", info1);
    test_stmt_err("select * from t", info2, error_code::permission_error);
    execute_statement("revoke select on table t from user1");
    test_stmt_err("select * from t", info1, error_code::permission_error);
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

    auto& smgr = *global::storage_manager();
    auto entry_opt = smgr.find_by_name("t");
    ASSERT_TRUE(entry_opt.has_value());
    auto entry = smgr.find_entry(entry_opt.value());
    ASSERT_TRUE(entry);
    auto& users_actions = entry->authorized_actions();
    auto& public_actions = entry->public_actions();
    EXPECT_TRUE(users_actions.find_user_actions("user2").has_action(action_kind::control));
    EXPECT_TRUE(users_actions.find_user_actions("user3").has_action(action_kind::select));

    execute_statement("revoke all privileges on table t from user2", info1);
    execute_statement("revoke select on table t from user3", info1);
    EXPECT_TRUE(! users_actions.find_user_actions("user2").has_action(action_kind::control));
    EXPECT_TRUE(! users_actions.find_user_actions("user3").has_action(action_kind::select));
}

TEST_F(sql_grant_revoke_test, grant_revoke_by_public_control) {
    // similar to grant_revoke_by_public_control, but with public privilege
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant all privileges on table t to public");
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("grant all privileges on table t to user2", info1);
    execute_statement("grant select on table t to user3", info1);

    auto& smgr = *global::storage_manager();
    auto entry_opt = smgr.find_by_name("t");
    ASSERT_TRUE(entry_opt.has_value());
    auto entry = smgr.find_entry(entry_opt.value());
    ASSERT_TRUE(entry);
    auto& users_actions = entry->authorized_actions();
    auto& public_actions = entry->public_actions();
    EXPECT_TRUE(users_actions.find_user_actions("user2").has_action(action_kind::control));
    EXPECT_TRUE(users_actions.find_user_actions("user3").has_action(action_kind::select));

    execute_statement("revoke all privileges on table t from user2", info1);
    execute_statement("revoke select on table t from user3", info1);
    EXPECT_TRUE(! users_actions.find_user_actions("user2").has_action(action_kind::control));
    EXPECT_TRUE(! users_actions.find_user_actions("user3").has_action(action_kind::select));
}

TEST_F(sql_grant_revoke_test, revoke_self) {
    // revoke allowed by control and it revoke itself
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant all privileges on table t to user1");
    auto info1 = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("revoke all privileges on table t from user1", info1);

    auto& smgr = *global::storage_manager();
    auto entry_opt = smgr.find_by_name("t");
    ASSERT_TRUE(entry_opt.has_value());
    auto entry = smgr.find_entry(entry_opt.value());
    ASSERT_TRUE(entry);
    auto& users_actions = entry->authorized_actions();
    auto& public_actions = entry->public_actions();
    EXPECT_TRUE(! users_actions.find_user_actions("user1").has_action(action_kind::control));
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
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info);
    execute_statement("revoke select,select,select on table t from user1");
    test_stmt_err("select * from t", info, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, grant_duplicate_users) {
    // verify no error in granting to same user multiple times
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t to user1,user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info);
    execute_statement("revoke select on table t from user1,user1");
    test_stmt_err("select * from t", info, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, grant_duplicate_tables) {
    // verify no error in granting privileges on the same table multiple times
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select on table t, t to user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info);
    execute_statement("revoke select on table t, t from user1");
    test_stmt_err("select * from t", info, error_code::permission_error);
}

TEST_F(sql_grant_revoke_test, grant_many_duplicates) {
    // verify no error in granting with many duplicates
    execute_statement("create table t (c0 int primary key)");
    execute_statement("grant select,select,select on table t, t, t to user1, user1, user1, public, public");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info);
    execute_statement("revoke select,select,select on table t, t, t from user1, user1, user1, public, public");
    test_stmt_err("select * from t", info, error_code::permission_error);
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

}  // namespace jogasaki::testing
