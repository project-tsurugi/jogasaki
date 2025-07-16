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

class sql_join_scan_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_join_scan(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
    bool has_join_scan(std::string_view query);
    bool uses_secondary(std::string_view query);
};

using namespace std::string_view_literals;

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

bool sql_join_scan_test::has_join_scan(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "join_scan");
}

bool sql_join_scan_test::uses_secondary(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "\"i1\"");
}

TEST_F(sql_join_scan_test, simple) {
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (1,10),(3,30)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 10)), result[0]);
}

TEST_F(sql_join_scan_test, left_outer) {
    execute_statement("CREATE TABLE t0 (c0 INT)");
    execute_statement("INSERT INTO t0 VALUES (1),(2),(4)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (1,10),(3,30),(4,40),(4,41)");
    {
        auto query = "select t0.c0, t1.c0, t1.c1 from t0 left outer join t1 on t0.c0=t1.c0";
        EXPECT_TRUE(has_join_scan(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>({1, 1, 10}, {false, false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>({2, -1, 10}, {false, true, true})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>({4, 4, 40}, {false, false, false})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>({4, 4, 41}, {false, false, false})), result[3]);
    }
}

TEST_F(sql_join_scan_test, right_outer) {
    // same as left_outer, but using RIGHT OUTER JOIN
    execute_statement("CREATE TABLE t0 (c0 INT)");
    execute_statement("INSERT INTO t0 VALUES (1),(2),(4)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (1,10),(3,30),(4,40),(4,41)");
    {
        auto query = "select t0.c0, t1.c0, t1.c1 from t1 right outer join t0 on t0.c0=t1.c0";
        EXPECT_TRUE(has_join_scan(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>({1, 1, 10}, {false, false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>({2, -1, 10}, {false, true, true})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>({4, 4, 40}, {false, false, false})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>({4, 4, 41}, {false, false, false})), result[3]);
    }
}

TEST_F(sql_join_scan_test, outer_join_with_condition) {
    execute_statement("create table L (C0 INT, C1 INT)");
    execute_statement("create table R (C0 INT, C1 INT, PRIMARY KEY(C0, C1))");
    execute_statement("INSERT INTO L (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (1, 1)");
    {
        auto query = "SELECT L.C0, L.C1, R.C0, R.C1 FROM L LEFT JOIN R ON L.C1=R.C0 AND L.C1 <> 1";
        EXPECT_TRUE(has_join_scan(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({1, 1, -1, -1}, {false, false, true, true})), result[0]);
    }
}

TEST_F(sql_join_scan_test, outer_join_with_condition_on_right_column) {
    execute_statement("create table L (C0 INT, C1 INT)");
    execute_statement("create table R (C0 INT, C1 INT, PRIMARY KEY(C0, C1))");
    execute_statement("INSERT INTO L (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (1, 1)");
    {
        auto query = "SELECT L.C0, L.C1, R.C0, R.C1 FROM L LEFT JOIN R ON L.C1=R.C0 AND R.C1 <> 1";
        EXPECT_TRUE(has_join_scan(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({1, 1, -1, -1}, {false, false, true, true})), result[0]);
    }
}

TEST_F(sql_join_scan_test, outer_join_with_condition_on_right_column_null) {
    execute_statement("create table L (C0 INT, C1 INT)");
    execute_statement("create table R (C0 INT, C1 INT, PRIMARY KEY(C0, C1))");
    execute_statement("INSERT INTO L (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (2, 2)");
    {
        auto query = "SELECT L.C0, L.C1, R.C0, R.C1 FROM L LEFT JOIN R ON L.C1=R.C0 AND R.C1 IS NULL";
        EXPECT_TRUE(has_join_scan(query));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({1, 1, -1, -1}, {false, false, true, true})), result[0]);
    }
}

TEST_F(sql_join_scan_test, use_secondary_index) {
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("CREATE INDEX i1 on t1 (c0, c1)");
    execute_statement("INSERT INTO t1 VALUES (1,10),(3,30)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 10)), result[0]);
}

TEST_F(sql_join_scan_test, left_outer_with_secondary_index) {
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("CREATE INDEX i1 on t1(c1, c0)");
    execute_statement("INSERT INTO t1 VALUES (10,1),(11,1)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 left outer join t1 on t0.c0=t1.c1";
    EXPECT_TRUE(has_join_scan(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 1)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 11, 1)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>({2, -1, -1}, {false, true, true})), result[2]);
}

TEST_F(sql_join_scan_test, use_secondary_index_with_null) {
    // verify null does not match with anything
    // primary index does not allow null on key columns, so test only with secondary index
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (null),(1)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("CREATE INDEX i1 on t1(c1, c0)");
    execute_statement("INSERT INTO t1 VALUES (10,null),(11,1)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c1";
    EXPECT_TRUE(has_join_scan(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 11, 1)), result[0]);
}

TEST_F(sql_join_scan_test, join_scan_disabled) {
    // verify fallback to shuffle join when join scan is disabled
    global::config_pool()->enable_join_scan(false);
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (1,10),(3,30)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(! has_join_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 10)), result[0]);
}

TEST_F(sql_join_scan_test, join_scan_multiple_records) {
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (1,10),(1,11),(2,20),(3,30)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(3, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 10)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 11)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 2, 20)), result[2]);
}

TEST_F(sql_join_scan_test, join_scan_secondary_with_nulls) {
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(null)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("CREATE INDEX i1 on t1 (c0, c1)");
    execute_statement("INSERT INTO t1 VALUES (1,10),(null,999)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 10)), result[0]);
}

TEST_F(sql_join_scan_test, join_scan_multiple_columns) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 bigint)");
    execute_statement("INSERT INTO t0 VALUES (1,11),(2,12)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 bigint, c2 int, primary key(c0, c1, c2))");
    execute_statement("INSERT INTO t1 VALUES (1,10,100),(1,11,100),(1,11,101),(2,20,200)");

    auto query = "SELECT t0.c0, t0.c1, t1.c0, t1.c1, t1.c2 FROM t0 join t1 on t0.c0=t1.c0 and t0.c1=t1.c1";
    EXPECT_TRUE(has_join_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8, kind::int4>(1,11,1,11,100)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8, kind::int4>(1,11,1,11,101)), result[1]);
}

TEST_F(sql_join_scan_test, join_scan_secondary_multiple_columns) {
    // similar as join_scan_multiple_columns but using secondary index
    execute_statement("CREATE TABLE t0 (c0 int, c1 bigint)");
    execute_statement("INSERT INTO t0 VALUES (1,11),(2,12)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 bigint, c2 int)");
    execute_statement("CREATE INDEX i1 on t1 (c0, c1, c2)");
    execute_statement("INSERT INTO t1 VALUES (1,10,100),(1,11,100),(1,11,101),(2,20,200)");

    auto query = "SELECT t0.c0, t0.c1, t1.c0, t1.c1, t1.c2 FROM t0 join t1 on t0.c0=t1.c0 and t0.c1=t1.c1";
    EXPECT_TRUE(has_join_scan(query));
    EXPECT_TRUE(uses_secondary(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8, kind::int4>(1,11,1,11,100)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8, kind::int4>(1,11,1,11,101)), result[1]);
}

// TODO add more tests for different types when issue #731 is resolved

TEST_F(sql_join_scan_test, different_type_double_vs_int) {
    // finding int key using double requires explicit type conversion
    execute_statement("CREATE TABLE t0 (c0 double)");
    execute_statement("INSERT INTO t0 VALUES (2147483647e0),(-2147483648e0)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (-2147483648,0),(2147483647,1)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    test_stmt_err(query, error_code::type_analyze_exception);
}

TEST_F(sql_join_scan_test, different_type_int_vs_double) {
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (2147483647),(-2147483648)");
    execute_statement("CREATE TABLE t1 (c0 double, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (-2147483648e0,0),(2147483647e0,1)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::float8, kind::int4>(-2147483648, -2147483648, 0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::float8, kind::int4>(2147483647, 2147483647, 1)), result[1]);
}

TEST_F(sql_join_scan_test, different_type_int_vs_bigint) {
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (2147483647),(-2147483648)");
    execute_statement("CREATE TABLE t1 (c0 bigint, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (-2147483649,-1), (-2147483648,0),(2147483647,1), (2147483648,2)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4>(-2147483648, -2147483648, 0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4>(2147483647, 2147483647, 1)), result[1]);
}

// TODO enable this test when issue #731 is resolved
TEST_F(sql_join_scan_test, DISABLED_different_type_bigint_vs_int) {
    execute_statement("CREATE TABLE t0 (c0 bigint)");
    execute_statement("INSERT INTO t0 VALUES (2147483648), (2147483647),(-2147483648), (-2147483649)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (-2147483648,0),(2147483647,1)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4>(-2147483648, -2147483648, 0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4>(2147483647, 2147483647, 1)), result[1]);
}

// TODO enable this test when issue #731 is resolved
TEST_F(sql_join_scan_test, DISABLED_different_type_decimal_vs_int) {
    execute_statement("CREATE TABLE t0 (c0 decimal(10))");
    execute_statement("INSERT INTO t0 VALUES (2147483648), (2147483647),(-2147483648), (-2147483649)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (-2147483648,0),(2147483647,1)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4>(-2147483648, -2147483648, 0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4>(2147483647, 2147483647, 1)), result[1]);
}

// TODO enable this test when issue #731 is resolved
TEST_F(sql_join_scan_test, DISABLED_different_type_int_vs_decimal) {
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (2147483648), (2147483647),(-2147483648), (-2147483649)");
    execute_statement("CREATE TABLE t1 (c0 decimal(10), c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (-2147483649,-1), (-2147483648,0),(2147483647,1), (2147483648,2)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4>(-2147483648, -2147483648, 0)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::int4>(2147483647, 2147483647, 1)), result[1]);
}
}
