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
#include <jogasaki/executor/describe.h>

#include <cstddef>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/basic_configurable_provider.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/secondary_index.h>
#include <jogasaki/utils/create_req_info.h>

#include "api_test_base.h"

namespace jogasaki::executor {
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;
using kind = meta::field_type_kind;
using api::impl::get_impl;

class explain_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
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
    void test_explain(
        std::string_view sql,
        std::string& out,
        std::shared_ptr<error::error_info>& error,
        request_info const& info
    );
};

using namespace std::string_view_literals;
using atom_type = dto::common_column::atom_type;

void explain_test::test_explain(
    std::string_view sql,
    std::string& out,
    std::shared_ptr<error::error_info>& error,
    request_info const& info
) {
    std::unique_ptr<api::executable_statement> executable{};
    ASSERT_EQ(status::ok, db_->create_executable(sql, executable));
    ASSERT_TRUE(executable);
    std::stringstream text{};
    auto st = global::database_impl()->explain(*executable, text, error, info);
    if(error) {
        ASSERT_EQ(st, error->status());
    }
    out = text.str();
}

TEST_F(explain_test, simple) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    std::string text{};
    std::shared_ptr<error::error_info> error{};
    test_explain("select * from t", text, error, {});
    EXPECT_TRUE(! error);
    EXPECT_TRUE(! text.empty());
}

TEST_F(explain_test, not_authorized) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    std::string text{};
    std::shared_ptr<error::error_info> error{};
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);
    test_explain("select * from t", text, error, info);
    ASSERT_TRUE(error);
    EXPECT_EQ(error_code::permission_error, error->code());
}

TEST_F(explain_test, authorization) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);

    // privileges that implies DESCRIBE authorization
    std::vector<std::string> privs = {
        "all privileges",
        "select",
        "insert",
        "update",
        "delete"
    };

    std::vector<std::string> grantee_types = {
        "public",
        "user1",
    };
    for(auto&& grantee : grantee_types) {
        for(auto&& priv : privs) {
            execute_statement("grant "+priv+" on t to "+grantee);

            std::string text{};
            std::shared_ptr<error::error_info> error{};
            test_explain("select * from t", text, error, info);
            EXPECT_TRUE(! error);
            EXPECT_TRUE(! text.empty());

            execute_statement("revoke "+priv+" on t from "+grantee);
        }
    }
}

}