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
#include <jogasaki/api/impl/database.h>

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

class list_tables_test :
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
};

using namespace std::string_view_literals;

auto list_tables(
    request_info const& req_info = {}
) {
    std::vector<std::string> tables{};
    std::shared_ptr<error::error_info> errors{};
    auto st = global::database_impl()->list_tables(tables, errors, req_info);
    std::sort(tables.begin(), tables.end());
    return std::tuple{st, tables, errors};
}

std::vector<std::string> tables{};
std::shared_ptr<error::error_info> errors{};

TEST_F(list_tables_test, simple) {
    execute_statement("create table t (c0 int primary key)");
    auto [st, tables, err] = list_tables();
    ASSERT_EQ(status::ok, st);
    ASSERT_TRUE(! err);

    std::vector<std::string> exp{
        "t"
    };
    EXPECT_EQ(exp, tables);
}

TEST_F(list_tables_test, multiple_tables) {
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("create table t1 (c0 int primary key)");
    execute_statement("create table t2 (c0 int primary key)");
    auto [st, tables, err] = list_tables();
    ASSERT_EQ(status::ok, st);
    ASSERT_TRUE(! err);

    std::vector<std::string> exp{
        "t0",
        "t1",
        "t2",
    };
    EXPECT_EQ(exp, tables);
}

TEST_F(list_tables_test, empty_result) {
    auto [st, tables, err] = list_tables();
    ASSERT_EQ(status::ok, st);
    ASSERT_TRUE(! err);

    std::vector<std::string> exp{};
    EXPECT_EQ(exp, tables);
}

TEST_F(list_tables_test, unauthorized_tables) {
    // tables are not listed if the user does not have any of CONTROL/SELECT/INSERT/UPDATE/DELETE privileges
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("create table t1 (c0 int primary key)");

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
            execute_statement("grant "+priv+" on t0 to "+grantee);
            auto info = utils::create_req_info("user1", tateyama::api::server::user_type::standard);

            auto [st, tables, err] = list_tables(info);
            ASSERT_EQ(status::ok, st);
            ASSERT_TRUE(! err);

            std::vector<std::string> exp{
                "t0"
            };
            EXPECT_EQ(exp, tables);
            execute_statement("revoke "+priv+" on t0 from "+grantee);
        }
    }
}

}
