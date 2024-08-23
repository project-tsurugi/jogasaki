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
#include <jogasaki/kvs/id.h>
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

class sql_insert_from_select_test :
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

TEST_F(sql_insert_from_select_test, simple) {
    execute_statement("create table t0 (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (2, 20), (3, 30)");
    execute_statement("create table t1 (c0 int primary key, c1 int)");
    execute_statement("insert into t1 select * from t0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 20)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(3, 30)), result[2]);
    }
}

TEST_F(sql_insert_from_select_test, column_name_does_not_matter) {
    // verify column names are not used to match the result and target column
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("create table t1 (c1 int, c0 int)");
    execute_statement("insert into t1 select * from t0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1, c0 FROM t1 ORDER BY c0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
    }
}

TEST_F(sql_insert_from_select_test, column_list_specified) {
    // verify column names are not used to match the result and target column
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("create table t1 (c1 int, c0 int)");
    execute_statement("insert into t1 (c0, c1) select * from t0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t1 ORDER BY c0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
    }
}

TEST_F(sql_insert_from_select_test, query_has_too_many_columns) {
    execute_statement("create table t0 (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("create table t1 (c0 int primary key)");
    test_stmt_err("insert into t1 select * from t0", error_code::analyze_exception);
}

TEST_F(sql_insert_from_select_test, table_has_less_columns_than_query) {
    // even though the query result can fit top columns of the target table, it is not allowed
    // column list should be specified when the number of columns does not match
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("INSERT INTO t0 VALUES (1)");
    execute_statement("create table t1 (c1 int primary key, c2 int default 100)");
    test_stmt_err("insert into t1 select * from t0", error_code::analyze_exception);
}

TEST_F(sql_insert_from_select_test, type_mismatch) {
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("INSERT INTO t0 VALUES (1)");
    execute_statement("create table t1 (c0 varchar(3) primary key)");
    test_stmt_err("insert into t1 select * from t0", error_code::type_analyze_exception);
}

TEST_F(sql_insert_from_select_test, complicated_column_order) {
    execute_statement("create table t0 (c0 int default 999, c1 int, c2 int, c3 int, primary key(c2, c1))");
    execute_statement("INSERT INTO t0 VALUES (1, 10, 100, 1000)");
    execute_statement("create table t1 (c0 int primary key, c1 int, c2 int, c3 int)");
    execute_statement("insert into t1 (c1, c3, c2, c0) select c1, c3, c2, c0 from t0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(1, 10, 100, 1000)), result[0]);
    }
}

TEST_F(sql_insert_from_select_test, default_value) {
    execute_statement("create table t0 (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (2, 20), (3, 30)");
    execute_statement("create table t1 (c0 int primary key, c1 int, c2 int default 100)");
    execute_statement("insert into t1 (c0, c1) select c0, c1 from t0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 100)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(3, 30, 100)), result[2]);
    }
}

TEST_F(sql_insert_from_select_test, pkless) {
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (2, 20), (3, 30)");
    execute_statement("create table t1 (c0 int, c1 int)");
    execute_statement("insert into t1 select * from t0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 20)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(3, 30)), result[2]);
    }
}

TEST_F(sql_insert_from_select_test, assign_conversion) {
    execute_statement("create table t0 (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (2, 20), (3, 30)");
    execute_statement("create table t1 (c0 int primary key, c1 real)");
    execute_statement("insert into t1 (c0, c1) select c0, c1 from t0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::float4>(1, 10.0)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::float4>(2, 20.0)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::float4>(3, 30.0)), result[2]);
    }
}

TEST_F(sql_insert_from_select_test, insert_or_replace) {
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (1, 20), (1, 30)");
    execute_statement("create table t1 (c0 int primary key, c1 int)");
    execute_statement("insert or replace into t1 select * from t0 order by c1");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 30)), result[0]);
    }
}

TEST_F(sql_insert_from_select_test, insert_or_replace_order_by_desc) {
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (1, 20), (1, 30)");
    execute_statement("create table t1 (c0 int primary key, c1 int)");
    execute_statement("insert or replace into t1 select * from t0 order by c1 desc");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
    }
}

TEST_F(sql_insert_from_select_test, insert_or_ignore) {
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (1, 20), (1, 30)");
    execute_statement("create table t1 (c0 int primary key, c1 int)");
    execute_statement("insert or ignore into t1 select * from t0 order by c1 desc");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 30)), result[0]);
    }
}

TEST_F(sql_insert_from_select_test, duplicate_pk) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback on error abort";
    }
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (2, 20), (2, 21)");
    execute_statement("create table t1 (c0 int primary key, c1 int)");
    test_stmt_err("insert into t1 select * from t0", error_code::unique_constraint_violation_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_insert_from_select_test, null) {
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 (c0) VALUES (1)"); // (c0, c1) = (1, null)
    execute_statement("create table t1 (c0 int, c1 int)");
    execute_statement("insert into t1 select null, c1 from t0");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>({0, 0}, {true, true})), result[0]);
    }
}

TEST_F(sql_insert_from_select_test, null_for_not_null) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback on error abort";
    }
    execute_statement("create table t0 (c0 int, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("create table t1 (c0 int primary key, c1 int)");
    test_stmt_err("insert into t1 select null, null from t0", error_code::sql_service_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t1 ORDER BY c0", result);
        ASSERT_EQ(0, result.size());
    }
}

}
