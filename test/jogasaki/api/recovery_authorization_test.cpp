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
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_req_info.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::string_literals;
using namespace std::string_view_literals;

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
};

TEST_F(recovery_authorization_test, table_authorization_persists_after_recovery) {
    // create table and check authorization after recovery
    auto info = utils::create_req_info("user1");
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)", info);

    // shutdown and restart database
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    auto entry = global::storage_manager()->find_by_name("t");
    ASSERT_TRUE(entry.has_value());

    auto sc = global::storage_manager()->find_entry(*entry);
    ASSERT_TRUE(sc);

    auto const& actions = sc->authorized_actions().find_user_actions("user1");
    EXPECT_TRUE(actions.action_allowed(auth::action_kind::control));
}

} // namespace jogasaki::testing
