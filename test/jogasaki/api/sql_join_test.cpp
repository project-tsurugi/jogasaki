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

class sql_join_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
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

static bool plan_contains(std::string_view plan, std::string_view token) {
    return plan.find(token) != std::string_view::npos;
}

TEST_F(sql_join_test, simple_join) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (1, 1)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t0 join t1 on t0.c1=t1.c1", result);
    ASSERT_EQ(1, result.size());
}

TEST_F(sql_join_test, cross_join) {
    execute_statement("CREATE TABLE T0 (C0 BIGINT PRIMARY KEY, C1 DOUBLE)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement("CREATE TABLE T10 (C0 BIGINT PRIMARY KEY, C1 DOUBLE)");
    execute_statement("INSERT INTO T10 (C0, C1) VALUES (3, 30.0)");
    execute_statement("INSERT INTO T10 (C0, C1) VALUES (4, 40.0)");
    execute_statement("INSERT INTO T10 (C0, C1) VALUES (5, 50.0)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0, T10", result);
    ASSERT_EQ(6, result.size());
}

TEST_F(sql_join_test, cross_join_pkless) {
    execute_statement("CREATE TABLE TT0(C0 INT)");
    execute_statement("INSERT INTO TT0 VALUES (10)");
    execute_statement("INSERT INTO TT0 VALUES (20)");
    execute_statement("CREATE TABLE TT1(C0 INT)");
    execute_statement("INSERT INTO TT1 VALUES (100)");
    execute_statement("INSERT INTO TT1 VALUES (200)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM TT0, TT1 ORDER BY TT0.C0, TT1.C0", result);
    ASSERT_EQ(4, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(10, 100)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(10, 200)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(20, 100)), result[2]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(20, 200)), result[3]);
}

TEST_F(sql_join_test, cross_join_pkless_multi_columns) {
    execute_statement("CREATE TABLE TT0(C0 INT, C1 INT)");
    execute_statement("INSERT INTO TT0 VALUES (10, 10)");
    execute_statement("INSERT INTO TT0 VALUES (20, 20)");
    execute_statement("CREATE TABLE TT1(C0 INT, C1 INT)");
    execute_statement("INSERT INTO TT1 VALUES (100, 100)");
    execute_statement("INSERT INTO TT1 VALUES (200, 200)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM TT0, TT1 ORDER BY TT0.C0, TT1.C0", result);
    ASSERT_EQ(4, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(10, 10, 100, 100)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(10, 10, 200, 200)), result[1]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(20, 20, 100, 100)), result[2]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(20, 20, 200, 200)), result[3]);
}

TEST_F(sql_join_test, cross_join_pkless_with_varchar) {
    // regression testcase - once mixing varchar column with hidden pk column caused server crash
    execute_statement("CREATE TABLE TT0(C0 VARCHAR(12))");
    execute_statement("INSERT INTO TT0 VALUES ('abcd')");
    execute_statement("INSERT INTO TT0 VALUES ('efgh')");
    execute_statement("CREATE TABLE TT1(C0 VARCHAR(12))");
    execute_statement("INSERT INTO TT1 VALUES ('AAAAA')");
    execute_statement("INSERT INTO TT1 VALUES ('BBBBBBB')");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM TT0, TT1 ORDER BY TT0.C0, TT1.C0", result);
    ASSERT_EQ(4, result.size());
    EXPECT_EQ(
        (mock::typed_nullable_record<kind::character, kind::character>(
            std::tuple{character_type(true, 12), character_type(true, 12)},
            accessor::text{"abcd"},
            accessor::text{"AAAAA"}
        )),
        result[0]
    );
    EXPECT_EQ(
        (mock::typed_nullable_record<kind::character, kind::character>(
            std::tuple{character_type(true, 12), character_type(true, 12)},
            accessor::text{"abcd"},
            accessor::text{"BBBBBBB"}
        )),
        result[1]
    );
    EXPECT_EQ(
        (mock::typed_nullable_record<kind::character, kind::character>(
            std::tuple{character_type(true, 12), character_type(true, 12)},
            accessor::text{"efgh"},
            accessor::text{"AAAAA"}
        )),
        result[2]
    );
    EXPECT_EQ(
        (mock::typed_nullable_record<kind::character, kind::character>(
            std::tuple{character_type(true, 12), character_type(true, 12)},
            accessor::text{"efgh"},
            accessor::text{"BBBBBBB"}
        )),
        result[3]
    );
}

TEST_F(sql_join_test, cross_join_with_no_columns) {
    // regression testcase (tsurugi-issues #794) - once cross join with no result columns involved 0 length record_ref
    // and caused wrong number of output records
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT 1 FROM t t0, t t1", result);
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(1)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(1)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(1)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(1)), result[3]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT t0.c0 FROM t t0, t t1 ORDER BY t0.c0", result);
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(2)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4>(2)), result[3]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT count(*) FROM t t0, t t1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(4)), result[0]);
    }
}

TEST_F(sql_join_test, outer_join) {
    execute_statement("create table L (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("create table R (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement( "INSERT INTO L (C0, C1) VALUES (1, 1)");
    execute_statement( "INSERT INTO L (C0, C1) VALUES (2, 2)");
    execute_statement( "INSERT INTO L (C0, C1) VALUES (3, 3)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (1, 1)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (30, 3)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (31, 3)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (4, 4)");

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, L.C1, R.C0, R.C1 FROM L LEFT JOIN R ON L.C1=R.C1 ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 1, 1, 1)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(2, 2, std::nullopt, std::nullopt)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 30, 3)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 31, 3)), result[3]);
    }
    {
        // same as above, but using RIGHT JOIN
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, L.C1, R.C0, R.C1 FROM R RIGHT JOIN L ON L.C1=R.C1 ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 1, 1, 1)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(2, 2, std::nullopt, std::nullopt)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 30, 3)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 31, 3)), result[3]);
    }
}

TEST_F(sql_join_test, outer_join_with_condition) {
    execute_statement("create table L (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("create table R (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement( "INSERT INTO L (C0, C1) VALUES (1, 1)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (1, 1)");

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, L.C1, R.C0, R.C1 FROM L LEFT JOIN R ON L.C1=R.C1 AND L.C1 <> 1 ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 1, std::nullopt, std::nullopt)), result[0]);
    }
}

TEST_F(sql_join_test, join_condition_on_clause) {
    // regression testcase - once join condition on clause caused wrong result
    execute_statement("CREATE TABLE TT0 (C0 INT NOT NULL, C1 INT NOT NULL, PRIMARY KEY(C0,C1))");
    execute_statement("CREATE TABLE TT1 (C0 INT NOT NULL, C1 INT NOT NULL, PRIMARY KEY(C0,C1))");
    execute_statement("INSERT INTO TT0 (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO TT1 (C0, C1) VALUES (1, 2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT0, TT1 WHERE TT0.C0=TT1.C0 AND TT0.C1 < TT1.C1", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT0 INNER JOIN TT1 ON TT0.C0=TT1.C0 WHERE TT0.C1 < TT1.C1", result);
        ASSERT_EQ(1, result.size());
    }
}

TEST_F(sql_join_test, join_key_different_types) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 bigint)");
    execute_statement("INSERT INTO t1 VALUES (1, 1)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t0 join t1 on t0.c1=t1.c1", result);
    ASSERT_EQ(1, result.size());
}

TEST_F(sql_join_test, join_key_different_types_decimal_float) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 decimal(5))");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 float)");
    execute_statement("INSERT INTO t1 VALUES (1, 1)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t0 join t1 on t0.c1=t1.c1", result);
    ASSERT_EQ(1, result.size());
}

TEST_F(sql_join_test, in_subquery_non_null_no_match_null_unsafe) {
    // issue #1486: t0.c1=3 IN (1, NULL) → unknown (no non-null match + null in subquery) → excluded.
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("INSERT INTO t0 VALUES (2, 3)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (10, 1)");
    execute_statement("INSERT INTO t1 VALUES (20, NULL)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT t0.c0 FROM t0 WHERE t0.c1 IN (SELECT c1 FROM t1) ORDER BY t0.c0", result);
    // c0=1: c1=1 matches t1.c1=1 → included
    // c0=2: c1=3 not in (1, NULL), no non-null match → unknown → excluded
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[0]);
}

TEST_F(sql_join_test, cross_join_with_where) {
    // regression testcase - once cross join with where clause caused server crash (issue 950)
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (1, 1)");

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t0 cross join t1 where t1.c1 = 1", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t0 cross join t1 where TRUE", result);
        ASSERT_EQ(1, result.size());
    }
}

TEST_F(sql_join_test, in_subquery_null_lhs_null_unsafe) {
    // issue #1486: IN (subquery) must use = semantics (null-unsafe).
    // t0.c1=NULL IN (SELECT c1 FROM t1) → unknown → row excluded.
    // Tables have no primary key to force shuffle-based semi join.
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("INSERT INTO t0 VALUES (2, NULL)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (10, 1)");
    execute_statement("INSERT INTO t1 VALUES (20, NULL)");

    auto const* query = "SELECT t0.c0 FROM t0 WHERE t0.c1 IN (SELECT c1 FROM t1) ORDER BY t0.c0";
    {
        std::string plan{};
        explain_statement(query, plan);
        EXPECT_TRUE(plan_contains(plan, "join_group")) << plan;
        EXPECT_TRUE(plan_contains(plan, "semi")) << plan;
    }
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    // Only c0=1 matches (c1=1 = c1=1); c0=2 has null c1 → unknown → excluded
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[0]);
}

TEST_F(sql_join_test, null_key_inner_join_null_unsafe) {
    // inner join with null join key drops the cogroup.
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("INSERT INTO t0 VALUES (2, NULL)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (1, 1)");
    execute_statement("INSERT INTO t1 VALUES (2, NULL)");

    auto const* query = "SELECT t0.c0, t0.c1, t1.c0, t1.c1 FROM t0 JOIN t1 ON t0.c1=t1.c1";
    {
        std::string plan{};
        explain_statement(query, plan);
        EXPECT_TRUE(plan_contains(plan, "join_group")) << plan;
        EXPECT_TRUE(plan_contains(plan, "inner")) << plan;
    }
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    // null-unsafe (=) semantics: null-key cogroup skipped, only non-null key rows match
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 1, 1, 1)), result[0]);
}

TEST_F(sql_join_test, intersect_distinct_null_safe) {
    // INTERSECT DISTINCT uses semi join + null-safe (NULL <=> NULL is true).
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("INSERT INTO t0 VALUES (2, NULL)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (10, 1)");
    execute_statement("INSERT INTO t1 VALUES (20, NULL)");

    auto const* query = "SELECT c1 FROM t0 INTERSECT DISTINCT SELECT c1 FROM t1";
    {
        std::string plan{};
        explain_statement(query, plan);
        EXPECT_TRUE(plan_contains(plan, "join_group")) << plan;
        EXPECT_TRUE(plan_contains(plan, "semi")) << plan;
    }
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    // null-safe: NULL <=> NULL → both 1 and NULL match
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((mock::create_nullable_record<kind::int4>(std::nullopt)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[1]);
}

TEST_F(sql_join_test, except_distinct_null_safe) {
    // EXCEPT DISTINCT uses anti join + null-safe (NULL <=> NULL is true).
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("INSERT INTO t0 VALUES (2, NULL)");
    execute_statement("INSERT INTO t0 VALUES (3, 2)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (10, 1)");
    execute_statement("INSERT INTO t1 VALUES (20, NULL)");

    auto const* query = "SELECT c1 FROM t0 EXCEPT DISTINCT SELECT c1 FROM t1";
    {
        std::string plan{};
        explain_statement(query, plan);
        EXPECT_TRUE(plan_contains(plan, "join_group")) << plan;
        EXPECT_TRUE(plan_contains(plan, "anti")) << plan;
    }
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    // null-safe: NULL in t1 excludes NULL from t0; only c1=2 remains
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4>(2)), result[0]);
}

}  // namespace jogasaki::testing
