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

} // namespace jogasaki::testing
