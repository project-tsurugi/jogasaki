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

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_except_test :
    public ::testing::Test,
    public api_test_base {

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

TEST_F(sql_except_test, except_distinct) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (0),(1),(2),(2)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("INSERT INTO t1 VALUES (1),(3)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("table t0 except distinct table t1", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[1]);
    }
}

TEST_F(sql_except_test, empty_input) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("INSERT INTO t1 VALUES (1),(3)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("table t0 except distinct table t1", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_except_test, simple) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (0)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("INSERT INTO t1 VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("table t0 except distinct table t1", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_except_test, nulls) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (null),(null),(2)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("INSERT INTO t1 VALUES (null),(1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("table t0 except distinct table t1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(sql_except_test, multiple_columns) {
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("create table t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (1, 10)");
    execute_statement("create table t2 (c0 int, c1 int)");
    execute_statement("INSERT INTO t2 VALUES (1, 1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("table t0 except distinct table t1", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("table t0 except distinct table t2", result);
        ASSERT_EQ(1, result.size());
    }
}

// enable when except all is supported
TEST_F(sql_except_test, DISABLED_except_all) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (0),(1),(2),(2)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("INSERT INTO t1 VALUES (1),(3)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("table t0 except all table t1", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[1]);
    }
}

}
