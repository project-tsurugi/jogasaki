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
    EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
        std::tuple{character_type(true, 12), character_type(true, 12)},
        std::forward_as_tuple(accessor::text{"abcd"}, accessor::text{"AAAAA"}))), result[0]);
    EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
        std::tuple{character_type(true, 12), character_type(true, 12)},
        std::forward_as_tuple(accessor::text{"abcd"}, accessor::text{"BBBBBBB"}))), result[1]);
    EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
        std::tuple{character_type(true, 12), character_type(true, 12)},
        std::forward_as_tuple(accessor::text{"efgh"}, accessor::text{"AAAAA"}))), result[2]);
    EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
        std::tuple{character_type(true, 12), character_type(true, 12)},
        std::forward_as_tuple(accessor::text{"efgh"}, accessor::text{"BBBBBBB"}))), result[3]);
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
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({2, 2, -1, -1}, {false, false, true, true})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 30, 3)), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 31, 3)), result[3]);
    }
    {
        // same as above, but using RIGHT JOIN
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, L.C1, R.C0, R.C1 FROM R RIGHT JOIN L ON L.C1=R.C1 ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 1, 1, 1)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({2, 2, -1, -1}, {false, false, true, true})), result[1]);
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
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({1, 1, -1, -1}, {false, false, true, true})), result[0]);
    }
}

TEST_F(sql_join_test, full_outer_join) {
    execute_statement("create table L (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("create table R (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement( "INSERT INTO L (C0, C1) VALUES (1, 1)");
    execute_statement( "INSERT INTO L (C0, C1) VALUES (2, 2)");
    execute_statement( "INSERT INTO L (C0, C1) VALUES (3, 3)");
    execute_statement( "INSERT INTO L (C0, C1) VALUES (50, 5)");
    execute_statement( "INSERT INTO L (C0, C1) VALUES (51, 5)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (1, 1)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (30, 3)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (31, 3)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (4, 4)");
    execute_statement( "INSERT INTO R (C0, C1) VALUES (5, 5)");

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, L.C1, R.C0, R.C1 FROM L FULL OUTER JOIN R ON L.C1=R.C1 ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(7, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({-1, -1, 4, 4}, {true, true, false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 1, 1, 1)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({2, 2, 0, 0}, {false, false, true, true})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 30, 3)), result[3]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 31, 3)), result[4]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(50, 5, 5, 5)), result[5]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(51, 5, 5, 5)), result[6]);
    }
    {
        // same as above, but L and R are replaced
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, L.C1, R.C0, R.C1 FROM R FULL OUTER JOIN L ON L.C1=R.C1 ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(7, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({-1, -1, 4, 4}, {true, true, false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 1, 1, 1)), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({2, 2, 0, 0}, {false, false, true, true})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 30, 3)), result[3]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(3, 3, 31, 3)), result[4]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(50, 5, 5, 5)), result[5]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(51, 5, 5, 5)), result[6]);
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

}
