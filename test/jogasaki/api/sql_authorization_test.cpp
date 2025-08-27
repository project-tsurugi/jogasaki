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
#include <jogasaki/executor/global.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_req_info.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::auth;

/**
 * @brief authorization testing not limited to GRANT/REVOKE statements
 */
class sql_authorization_test :
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

TEST_F(sql_authorization_test, control_privilege_on_create_table) {
    auto info = utils::create_req_info("user1");
    execute_statement("create table t (c0 int primary key)", info);
    auto& smgr = *global::storage_manager();
    auto entry_opt = smgr.find_by_name("t");
    ASSERT_TRUE(entry_opt.has_value());
    auto entry = smgr.find_entry(entry_opt.value());
    ASSERT_TRUE(entry);
    auto& users_actions = entry->authorized_actions();
    auto actions = users_actions.find_user_actions("user1");
    EXPECT_TRUE(actions.has_action(action_kind::control));
}

void grant(action_set actions, std::string_view table_name, std::string_view user) {
    auto& smgr = *global::storage_manager();
    auto entry_opt = smgr.find_by_name(table_name);
    ASSERT_TRUE(entry_opt.has_value());
    auto entry = smgr.find_entry(entry_opt.value());
    ASSERT_TRUE(entry);
    if (user == "public") {
        auto& public_actions = entry->public_actions();
        public_actions.add_actions(std::move(actions));
        return;
    }
    auto& users_actions = entry->authorized_actions();
    users_actions.add_user_actions(user, std::move(actions));
}

void revoke(action_set actions, std::string_view table_name, std::string_view user) {
    auto& smgr = *global::storage_manager();
    auto entry_opt = smgr.find_by_name(table_name);
    ASSERT_TRUE(entry_opt.has_value());
    auto entry = smgr.find_entry(entry_opt.value());
    ASSERT_TRUE(entry);
    if (user == "public") {
        auto& public_actions = entry->public_actions();
        for(auto&& e : actions) {
            public_actions.remove_action(e);
        }
        return;
    }
    auto& users_actions = entry->authorized_actions();
    for(auto&& e : actions) {
        users_actions.remove_user_action(user, e);
    }
}

TEST_F(sql_authorization_test, select) {
    execute_statement("create table t (c0 int primary key)");
    grant(action_set{action_kind::select}, "t", "user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info);
}

TEST_F(sql_authorization_test, select_fail) {
    execute_statement("create table t (c0 int primary key)");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("select * from t", info, error_code::permission_error);
}

TEST_F(sql_authorization_test, select_by_public_privilege) {
    execute_statement("create table t (c0 int primary key)");
    grant(action_set{action_kind::select}, "t", "public");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info);
}

TEST_F(sql_authorization_test, insert) {
    execute_statement("create table t (c0 int primary key)");
    grant(action_set{action_kind::insert}, "t", "user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("insert into t values (1)", info);
}

TEST_F(sql_authorization_test, insert_fail) {
    execute_statement("create table t (c0 int primary key)");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("insert into t values (1)", info, error_code::permission_error);
}

TEST_F(sql_authorization_test, update) {
    execute_statement("create table t (c0 int primary key)");
    grant(action_set{action_kind::update, action_kind::select}, "t", "user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("update t set c0=2", info);
}

TEST_F(sql_authorization_test, update_fail) {
    execute_statement("create table t (c0 int primary key)");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("update t set c0=2", info, error_code::permission_error);
}

TEST_F(sql_authorization_test, delete) {
    execute_statement("create table t (c0 int primary key)");
    grant(action_set{action_kind::delete_, action_kind::select}, "t", "user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("delete from t", info);
}

TEST_F(sql_authorization_test, delete_fail) {
    execute_statement("create table t (c0 int primary key)");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("delete from t", info, error_code::permission_error);
}

TEST_F(sql_authorization_test, revoke_control) {
    auto info = utils::create_req_info("user1");
    execute_statement("create table t (c0 int primary key)", info);
    revoke(action_set{action_kind::control}, "t", "user1");
    // re-define user1 as standard user in order to test auth
    auto standard_user_info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("select * from t", standard_user_info, error_code::permission_error);
}

TEST_F(sql_authorization_test, revoke_select) {
    execute_statement("create table t (c0 int primary key)");
    grant(action_set{action_kind::select}, "t", "user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("select * from t", info);
    revoke(action_set{action_kind::select}, "t", "user1");
    test_stmt_err("select * from t", info, error_code::permission_error);
}

TEST_F(sql_authorization_test, create_table_fail) {
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("create table t (c0 int primary key)", info, error_code::permission_error);
}

TEST_F(sql_authorization_test, drop_table_fail) {
    execute_statement("create table t (c0 int primary key)");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("drop table t", info, error_code::permission_error);
}

TEST_F(sql_authorization_test, drop_table_success_by_control) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("grant all privileges on table t to user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("drop table t", info);
}

TEST_F(sql_authorization_test, drop_table_success_by_public_control) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("grant all privileges on table t to public");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("drop table t", info);
}

TEST_F(sql_authorization_test, create_index_fail) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_stmt_err("create index i on t (c1)", info, error_code::permission_error);
}

TEST_F(sql_authorization_test, create_index_success_by_control) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("grant all privileges on table t to user1");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("create index i on t (c1)", info);
}

TEST_F(sql_authorization_test, create_index_success_by_public_control) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("grant all privileges on table t to public");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    execute_statement("create index i on t (c1)", info);
}

} // namespace jogasaki::testing
