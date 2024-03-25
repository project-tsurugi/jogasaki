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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/configuration.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/data/any.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/process/impl/expression/details/constants.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/utils/storage_data.h>

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
    global::config_pool()->normalize_float(false);
    execute_statement("create table t (c0 DOUBLE primary key)");

    execute_statement("insert into t values (CAST('-0' AS DOUBLE))");
    execute_statement("insert into t values (CAST('0' AS DOUBLE))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select CAST(t0.c0 AS VARCHAR(*)), CAST(t1.c0 AS VARCHAR(*)) from t t0 join t t1 on t0.c0=t1.c0", result);
        // FIXME verify result
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::character, kind::character>({text{"-0"}, text{"-0"}}, {false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::character, kind::character>({text{"-0"}, text{"-0"}}, {false, false})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::character, kind::character>({text{"-0"}, text{"-0"}}, {false, false})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::character, kind::character>({text{"-0"}, text{"-0"}}, {false, false})), result[3]);
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
