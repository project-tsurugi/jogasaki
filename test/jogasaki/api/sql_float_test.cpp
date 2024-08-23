/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

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

class sql_float_test :
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

using jogasaki::accessor::text;

TEST_F(sql_float_test, inserting_zeroes) {
    // verify positive zero and negative zero conflict when stored
    execute_statement("create table t (c0 DOUBLE primary key)");
    execute_statement("insert into t values (CAST('-0' AS DOUBLE))");
    test_stmt_err("insert into t values (CAST('0' AS DOUBLE))", error_code::unique_constraint_violation_exception);
}

TEST_F(sql_float_test, join_by_positive_negative_zeros_comparison) {
    // regression testcase - once the result became [{-0, -0}, {-0, -0}, {-0, -0}, {-0, -0}]
    // usually -0 is normalized to 0, so it doesn't matter that join result contains "-0" since it's converted to 0.
    // This testcase is left to verify that the original values (i.e. "-0" or "+0") are preserved.
    // Even this testcase is broken, it doesn't necessarily mean that the feature is broken but it's worth to investigate
    // why the original values are not preserved.
    global::config_pool()->normalize_float(false);
    execute_statement("create table t (c0 DOUBLE)");

    execute_statement("insert into t values (CAST('-0' AS DOUBLE))");
    execute_statement("insert into t values (CAST('0' AS DOUBLE))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select CAST(t0.c0 AS VARCHAR(*)), CAST(t1.c0 AS VARCHAR(*)) from t t0 join t t1 on t0.c0=t1.c0", result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::character, kind::character>({text{"-0"}, text{"-0"}}, {false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::character, kind::character>({text{"-0"}, text{"0"}}, {false, false})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::character, kind::character>({text{"0"}, text{"-0"}}, {false, false})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::character, kind::character>({text{"0"}, text{"0"}}, {false, false})), result[3]);
    }
}

TEST_F(sql_float_test, inserting_nans) {
    // verify various nans conflict when stored
    execute_statement("create table t (c0 DOUBLE primary key)");
    execute_statement("insert into t values (CAST('NaN' AS DOUBLE))");
    test_stmt_err("insert into t values (CAST('NaN' AS DOUBLE))", error_code::unique_constraint_violation_exception);
    test_stmt_err("insert into t values (CAST('-NaN' AS DOUBLE))", error_code::unique_constraint_violation_exception);
    test_stmt_err("insert into t values (-CAST('NaN' AS DOUBLE))", error_code::unique_constraint_violation_exception);
    test_stmt_err("insert into t values (CAST('Infinity' AS DOUBLE) / CAST('Infinity' AS DOUBLE))", error_code::unique_constraint_violation_exception);
    test_stmt_err("insert into t values (CAST('-Infinity' AS DOUBLE) / CAST('Infinity' AS DOUBLE))", error_code::unique_constraint_violation_exception);
    test_stmt_err("insert into t values (CAST('-Infinity' AS DOUBLE) / CAST('-Infinity' AS DOUBLE))", error_code::unique_constraint_violation_exception);
}

TEST_F(sql_float_test, join_on_nans) {
    // verify nan equals itself both on join condition and where condition
    execute_statement("create table t (c0 DOUBLE primary key)");
    execute_statement("insert into t values (CAST('NaN' AS DOUBLE))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t t0 join t t1 on t0.c0=t1.c0", result);
        EXPECT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t where c0 = c0", result);
        EXPECT_EQ(1, result.size());
    }
}

TEST_F(sql_float_test, order_float_values) {
    execute_statement("create table t (c0 DOUBLE)");
    execute_statement("insert into t values (CAST('-Infinity' AS DOUBLE))");
    execute_statement("insert into t values (CAST('0' AS DOUBLE))");
    execute_statement("insert into t values (CAST('+Infinity' AS DOUBLE))");
    execute_statement("insert into t values (CAST('NaN' AS DOUBLE))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select CAST(c0 AS VARCHAR(*)) from t order by c0", result);
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::character>({text{"-Infinity"}}, {false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::character>({text{"0"}}, {false})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::character>({text{"Infinity"}}, {false})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::character>({text{"NaN"}}, {false})), result[3]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select CAST(c0 AS VARCHAR(*)) from t order by c0 desc", result);
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::character>({text{"NaN"}}, {false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::character>({text{"Infinity"}}, {false})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::character>({text{"0"}}, {false})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::character>({text{"-Infinity"}}, {false})), result[3]);
    }
}

}  // namespace jogasaki::testing
