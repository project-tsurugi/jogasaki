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

class sql_full_outer_join_test :
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

TEST_F(sql_full_outer_join_test, simple) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (1, 1)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t0 full outer join t1 on t0.c1=t1.c1", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 1, 1, 1)), result[0]);
}

TEST_F(sql_full_outer_join_test, against_empty_table) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (1, 1)");

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT t0.c0, t0.c1, t1.c0, t1.c1 FROM t0 full outer join t1 on t0.c1=t1.c1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({-1, -1, 1, 1}, {true, true, false, false})), result[0]);
    }
    {
        // same as above except left and right are swapped
        std::vector<mock::basic_record> result{};
        execute_query("SELECT t0.c0, t0.c1, t1.c0, t1.c1 FROM t1 full outer join t0 on t0.c1=t1.c1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({-1, -1, 1, 1}, {true, true, false, false})), result[0]);
    }
}

TEST_F(sql_full_outer_join_test, both_sides_empty) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t0 full outer join t1 on t0.c1=t1.c1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_full_outer_join_test, join_with_condition) {
    execute_statement("create table L (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("create table R (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO L (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (1, 1)");

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, L.C1, R.C0, R.C1 FROM L FULL JOIN R ON L.C1=R.C1 AND L.C1 <> 1 ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({-1, -1, 1, 1}, {true, true, false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({1, 1, -1, -1}, {false, false, true, true})), result[1]);
    }
}

TEST_F(sql_full_outer_join_test, join_condition_always_false) {
    // regression testcase - once join condition additional to equivalence caused wrong result
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (2, 2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT t0.c0, t0.c1, t1.c0, t1.c1 FROM t0 full outer join t1 on t0.c1=t1.c1 and FALSE", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({-1, -1, 2, 2}, {true, true, false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>({1, 1, -1, -1}, {false, false, true, true})), result[1]);
    }
}

TEST_F(sql_full_outer_join_test, matched_or_unmatched_by_condition) {
    execute_statement("create table L (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("create table R (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO L (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO L (C0, C1) VALUES (2, 1)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (10, 1)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (20, 1)");

    // equivalence condition is always met as 1 = 1
    {
        // some record has matched condition, others not
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, R.C0 FROM L FULL JOIN R ON L.C1=R.C1 AND L.C0 <> 1 AND R.C0 <> 10 ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({-1, 10}, {true, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({1, -1}, {false, true})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({2, 20}, {false, false})), result[2]);
    }
    {
        // condition always false
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, R.C0 FROM L FULL JOIN R ON L.C1=R.C1 AND FALSE ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({-1, 10}, {true, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({-1, 20}, {true, false})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({1, -1}, {false, true})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({2, -1}, {false, true})), result[3]);
    }
    {
        // condition always true
        std::vector<mock::basic_record> result{};
        execute_query("SELECT L.C0, R.C0 FROM L FULL JOIN R ON L.C1=R.C1 AND TRUE ORDER BY L.C0, R.C0", result);
        ASSERT_EQ(4, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({1, 10}, {false, false})), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({1, 20}, {false, false})), result[1]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({2, 10}, {false, false})), result[2]);
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>({2, 20}, {false, false})), result[3]);
    }
}

TEST_F(sql_full_outer_join_test, basic) {
    execute_statement("create table L (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("create table R (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO L (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO L (C0, C1) VALUES (2, 2)");
    execute_statement("INSERT INTO L (C0, C1) VALUES (3, 3)");
    execute_statement("INSERT INTO L (C0, C1) VALUES (50, 5)");
    execute_statement("INSERT INTO L (C0, C1) VALUES (51, 5)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (30, 3)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (31, 3)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (4, 4)");
    execute_statement("INSERT INTO R (C0, C1) VALUES (5, 5)");

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

TEST_F(sql_full_outer_join_test, join_key_different_types) {
    execute_statement("CREATE TABLE t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 1)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 bigint)");
    execute_statement("INSERT INTO t1 VALUES (1, 1)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t0 full outer join t1 on t0.c1=t1.c1", result);
    ASSERT_EQ(1, result.size());
}

}
