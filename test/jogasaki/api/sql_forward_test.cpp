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
#include <jogasaki/executor/global.h>
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

// test forward exchange via sql api
class sql_forward_test :
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

TEST_F(sql_forward_test, limit_without_order_by) {
    execute_statement("create table t (C0 int)");
    execute_statement("insert into t values (10), (10), (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t LIMIT 2", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), result[1]);
    }
}

TEST_F(sql_forward_test, limit_without_order_by_zero) {
    execute_statement("create table t (C0 int)");
    execute_statement("insert into t values (10), (10), (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t LIMIT 0", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_forward_test, union_all) {
    execute_statement("create table t1 (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (1,10)");
    execute_statement("create table t2 (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t2 VALUES (2,20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 from t1 UNION ALL SELECT c1, c0 from t2", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(20, 2)), result[1]);
    }
}

TEST_F(sql_forward_test, union_all_with_same_table) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t VALUES (1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 from t UNION ALL SELECT c1, c0 from t", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 1)), result[1]);
    }
}

TEST_F(sql_forward_test, union_all_same_table_3_times) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t VALUES (1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("(SELECT c0, c0 from t UNION ALL SELECT c0, c1 from t ) UNION ALL SELECT c1, c1 from t", result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 10)), result[2]);
    }
}

TEST_F(sql_forward_test, union_all_same_table_4_times_wide) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t VALUES (1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("(SELECT c0, c0 from t UNION ALL SELECT c0, c1 from t ) UNION ALL (SELECT c1, c0 from t UNION ALL SELECT c1, c1 from t)", result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 1)), result[2]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 10)), result[3]);
    }
}

TEST_F(sql_forward_test, union_all_same_table_4_times_deep) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t VALUES (1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("((SELECT c0, c0 from t UNION ALL SELECT c0, c1 from t ) UNION ALL (SELECT c1, c0 from t)) UNION ALL SELECT c1, c1 from t", result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 1)), result[2]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 10)), result[3]);
    }
}

TEST_F(sql_forward_test, complex) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t VALUES (1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query(
        "SELECT c0, c0 from t"
        " UNION DISTINCT"
        " SELECT c0, c1 from t limit 1"
        " UNION ALL"
        " SELECT c0, c1 from t limit 1"
        " UNION ALL"
        " SELECT max(c0), c1 from t group by c1 limit 1"
        " UNION ALL"
        " SELECT c1, max(c0) from t group by c1"
        , result);
        ASSERT_EQ(5, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[2]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[3]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(10, 1)), result[4]);
    }
}

TEST_F(sql_forward_test, different_types_int_decimal) {
    execute_statement("create table t (c0 int primary key, c1 decimal(38))");
    execute_statement("INSERT INTO t VALUES (1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query(
        "SELECT c0, c1 from t"
        " UNION ALL"
        " SELECT c1, c0 from t"
        , result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal, kind::decimal>(
            std::tuple{
                meta::decimal_type(38, 0),
                meta::decimal_type(38, 0),
            }, {
                triple{1, 0, 1, 0},
                triple{1, 0, 10, 0},
            }
        )), result[0]);
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal, kind::decimal>(
            std::tuple{
                meta::decimal_type(38, 0),
                meta::decimal_type(38, 0),
            }, {
                triple{1, 0, 10, 0},
                triple{1, 0, 1, 0},
            }
        )), result[1]);
    }
}

TEST_F(sql_forward_test, different_types_int_bigint) {
    execute_statement("create table t (c0 int primary key, c1 bigint)");
    execute_statement("INSERT INTO t VALUES (1,10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query(
        "SELECT c0, c1 from t"
        " UNION ALL"
        " SELECT c1, c0 from t"
        , result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1, 10)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(10, 1)), result[1]);
    }
}


// enable when issue 943 is completed
TEST_F(sql_forward_test, DISABLED_union_join) {
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1,10)");
    execute_statement("INSERT INTO t1 VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t0 union join t1", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_forward_test, union_two_tables_with_different_number_of_columns) {
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1,10)");
    execute_statement("INSERT INTO t1 VALUES (1)");
    test_stmt_err("select * from t0 union all select * from t1", error_code::analyze_exception);
}

}
