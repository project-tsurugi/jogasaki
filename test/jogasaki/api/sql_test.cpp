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

class sql_test :
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

TEST_F(sql_test, update_by_part_of_primary_key) {
    execute_statement("CREATE TABLE T20 (C0 BIGINT, C1 INT DEFAULT 0, C2 DOUBLE, C3 FLOAT, C4 VARCHAR(100), PRIMARY KEY(C0, C1))");
    execute_statement("INSERT INTO T20 (C0, C2, C4) VALUES (1, 100.0, '111')");
    execute_statement("UPDATE T20 SET C2=200.0 WHERE C0=1");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1, C2 FROM T20", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(1, rec.get_value<std::int64_t>(0));
    EXPECT_EQ(0, rec.get_value<std::int32_t>(1));
    EXPECT_DOUBLE_EQ(200.0, rec.get_value<double>(2));
    EXPECT_FALSE(rec.is_null(2));
}

TEST_F(sql_test, update_primary_key) {
    execute_statement("CREATE TABLE T0 (C0 BIGINT PRIMARY KEY, C1 DOUBLE)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement("UPDATE T0 SET C0=3, C1=30.0 WHERE C1=10.0");
    wait_epochs(2);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    auto meta = result[0].record_meta();
    EXPECT_EQ(2, result[0].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(20.0, result[0].get_value<double>(1));
    EXPECT_EQ(3, result[1].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(30.0, result[1].get_value<double>(1));
}

TEST_F(sql_test, read_null) {
    execute_statement("CREATE TABLE T0 (C0 BIGINT PRIMARY KEY, C1 DOUBLE)");
    execute_statement("INSERT INTO T0(C0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>({0, 0.0}, {false, true})), result[0]);
    }
}

TEST_F(sql_test, literal_true_false) {
    execute_statement("CREATE TABLE T0 (C0 BIGINT PRIMARY KEY, C1 DOUBLE)");
    execute_statement("INSERT INTO T0(C0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1, TRUE, FALSE FROM T0 WHERE TRUE", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (create_nullable_record<kind::int8, kind::float8, kind::boolean, kind::boolean>(
                {0, 0.0, true, false},
                {false, true, false, false}
            )),
            result[0]
        );
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1, TRUE FROM T0 WHERE FALSE", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_test, subquery) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int)");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1,1)");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (2,2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from (select * from TT t00, TT t01) t1", result);
        ASSERT_EQ(4, result.size());
    }
}

TEST_F(sql_test, select_distinct) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int, C2 int)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (1,1,1)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (2,1,1)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (3,1,2)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (4,1,NULL)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (5,1,NULL)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select distinct C1 from TT", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select distinct C1, C2 from TT", result);
        ASSERT_EQ(3, result.size());
    }
}

TEST_F(sql_test, select_group_by_for_distinct) {
    // same as select_distinct using group by
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int, C2 int)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (1,1,1)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (2,1,1)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (3,1,2)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (4,1,NULL)");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (5,1,NULL)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select C1 from TT group by C1", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select distinct C1, C2 from TT group by C1, C2", result);
        ASSERT_EQ(3, result.size());
    }
}

TEST_F(sql_test, select_constant) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int)");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1,1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select 1 from TT", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select true from TT", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select false from TT", result);
        ASSERT_EQ(1, result.size());
    }
}

// like expression not yet supported
TEST_F(sql_test, select_boolean_expression) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 VARCHAR(10))");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1, 'ABC')");
    test_stmt_err("select C1 like 'A%' from TT", error_code::unsupported_runtime_feature_exception);
    // {
        // std::vector<mock::basic_record> result{};
        // execute_query("select C1 like 'A%' from TT", result);
        // ASSERT_EQ(1, result.size());
    // }
}

// like expression not yet supported
TEST_F(sql_test, like_expression) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 VARCHAR(10))");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1, 'ABC')");
    test_stmt_err("select * from TT where C1 like 'A%'", error_code::unsupported_runtime_feature_exception);
    // {
        // std::vector<mock::basic_record> result{};
        // execute_query("select * from TT where C1 like 'A%'", result);
        // ASSERT_EQ(1, result.size());
    // }
    test_stmt_err("select * from TT where NOT C1 like 'A%'", error_code::unsupported_runtime_feature_exception);
    // {
        // std::vector<mock::basic_record> result{};
        // execute_query("select * from TT where NOT C1 like 'A%'", result);
        // ASSERT_EQ(0, result.size());
    // }
}

TEST_F(sql_test, double_literal) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 double)");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1, 1e2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select 1e-2 from TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::float8>(0.01)), result[0]);
    }
}

// currently we want to support sql up to 2GB, but failed with oom in syntax verification. Check after compiler is upgraded.
TEST_F(sql_test, DISABLED_long_sql) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT (C0 int primary key, C1 int)");
    execute_statement("INSERT INTO TT (C0, C1) VALUES (1,1)");
    {
        std::string blanks(2*1024*1024*1024UL - 20UL, ' ');
        std::vector<mock::basic_record> result{};
        execute_query("select * " + blanks + "from TT", result);
        ASSERT_EQ(1, result.size());
    }
}

TEST_F(sql_test, is_null) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int, C1 int)");
    execute_statement("INSERT INTO T (C0) VALUES (1)");
    execute_statement("INSERT INTO T (C0,C1) VALUES (2, 20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 IS NULL ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 IS NOT NULL ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(sql_test, is_true) {
    execute_statement("create table T (C0 int, C1 int)");
    execute_statement("INSERT INTO T (C0,C1) VALUES (1, 10)");
    execute_statement("INSERT INTO T (C0,C1) VALUES (2, 20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 10 IS TRUE", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 10 IS NOT TRUE", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(sql_test, is_true_with_null) {
    execute_statement("create table T (C0 int, C1 int)");
    execute_statement("INSERT INTO T (C0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 10 IS TRUE", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 10 IS NOT TRUE", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
}

TEST_F(sql_test, is_false) {
    execute_statement("create table T (C0 int, C1 int)");
    execute_statement("INSERT INTO T (C0,C1) VALUES (1, 10)");
    execute_statement("INSERT INTO T (C0,C1) VALUES (2, 20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 10 IS FALSE", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 10 IS NOT FALSE", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
}

TEST_F(sql_test, is_false_with_null) {
    execute_statement("create table T (C0 int, C1 int)");
    execute_statement("INSERT INTO T (C0) VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 10 IS FALSE", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 10 IS NOT FALSE", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
}

TEST_F(sql_test, is_unknown) {
    execute_statement("create table T (C0 int, C1 int)");
    execute_statement("INSERT INTO T (C0) VALUES (1)");
    execute_statement("INSERT INTO T (C0,C1) VALUES (2, 20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 0 IS UNKNOWN ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T WHERE C1 = 0 IS NOT UNKNOWN ORDER BY C0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(sql_test, literal_with_invalid_char) {
    // old compiler failed to handle invalid char such as $1
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int)");
    execute_statement("INSERT INTO T (C0) VALUES (1)");
    test_stmt_err("SELECT C0 FROM T WHERE C0=$1", error_code::syntax_exception);
}

TEST_F(sql_test, insert_string_with_invalid_char) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 varchar(3) primary key)");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character},
    };
    auto ps = api::create_parameter_set();
    std::string str("A\0B", 3);
    ps->set_character("p0", str);

    test_stmt_err("INSERT INTO T VALUES (:p0)", variables, *ps, error_code::invalid_runtime_value_exception);
}

TEST_F(sql_test, update_string_with_invalid_char) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 varchar(3) primary key)");
    execute_statement("INSERT INTO T VALUES ('ABC')");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character},
    };
    auto ps = api::create_parameter_set();
    std::string str("A\0B", 3);
    ps->set_character("p0", str);

    test_stmt_err("UPDATE T SET C0 = :p0", variables, *ps, error_code::invalid_runtime_value_exception);
}

TEST_F(sql_test, select_null) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int primary key)");
    execute_statement("INSERT INTO T VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, NULL FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::unknown>(std::tuple{1, '\0'}, {false, true})), result[0]);
    }
}

TEST_F(sql_test, unsupported_features) {
    test_stmt_err("select 1", error_code::unsupported_compiler_feature_exception);
    test_stmt_err("values (1)", error_code::unsupported_compiler_feature_exception);
}

TEST_F(sql_test, limit) {
    // test limit clause with order by (i.e. group operator is used)
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (0), (1), (2), (3), (4)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t ORDER BY c0 LIMIT 2", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[1]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t ORDER BY c0 DESC LIMIT 2", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(4)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[1]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t ORDER BY c0 ASC LIMIT 9223372036854775807", result);
        ASSERT_EQ(5, result.size());
    }
    test_stmt_err("SELECT c0 FROM t ORDER BY c0 ASC LIMIT 9223372036854775808", error_code::type_analyze_exception);
    test_stmt_err("SELECT c0 FROM t ORDER BY c0 ASC LIMIT -1", error_code::type_analyze_exception);
    test_stmt_err("SELECT c0 FROM t ORDER BY c0 ASC LIMIT -9223372036854775808", error_code::type_analyze_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t ORDER BY c0 LIMIT 0", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_test, simple_having) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t VALUES "
        "(11,10),(12,10),"
        "(21,20),(22,20),"
        "(31,30),(32,30)"
    );
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT sum(c0), c1 FROM t group by c1 having sum(c0) > 60", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(63, 30)), result[0]);
    }
}

TEST_F(sql_test, having_witout_group_by) {
    execute_statement("create table t (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t VALUES "
        "(11,10),(12,10),"
        "(21,20),(22,20),"
        "(31,30),(32,30)"
    );
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT sum(c0), sum(c1) FROM t having sum(c0) > 0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(129, 120)), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT sum(c0), sum(c1) FROM t having sum(c0) < 0", result);
        ASSERT_EQ(0, result.size());
    }
}

// TODO enable when compiler supports EXISTS
TEST_F(sql_test, DISABLED_exists) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (0),(1)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("INSERT INTO t1 VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t0 where exists (select * from t1)", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[1]);
    }
}

TEST_F(sql_test, case_expression) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (0),(1),(2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select case c0 when 0 then 100 when 1 then 101 else 200 end from t0", result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int8>(100)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int8>(101)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int8>(200)), result[2]);
    }
}

TEST_F(sql_test, case_expression_search) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (-1),(0),(1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select case when c0 > 0 then 1 when c0 < 0 then -1 else 0 end from t0", result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int8>(-1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int8>(0)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int8>(1)), result[2]);
    }
}

TEST_F(sql_test, case_expression_with_type_conversion) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (-1),(0),(1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select case when c0 > 0 then cast(1 as INT) when c0 < 0 then cast(-1 as DECIMAL(5)) else CAST(0 as BIGINT) end from t0", result);
        ASSERT_EQ(3, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((typed_nullable_record<kind::decimal>(std::tuple{meta::decimal_type(19,0)}, std::forward_as_tuple(triple{-1, 0, 1, 0}))), result[0]);
        EXPECT_EQ((typed_nullable_record<kind::decimal>(std::tuple{meta::decimal_type(19,0)}, std::forward_as_tuple(triple{1, 0, 0, 0}))), result[1]);
        EXPECT_EQ((typed_nullable_record<kind::decimal>(std::tuple{meta::decimal_type(19,0)}, std::forward_as_tuple(triple{1, 0, 1, 0}))), result[2]);
    }
}

TEST_F(sql_test, case_expression_using_variable_in_body) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (0),(1),(2),(null)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select case c0 when 0 then c0 + 1 when 1 then c0 * 2 else 200 end from t0", result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int8>(1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int8>(2)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int8>(200)), result[2]);
        EXPECT_EQ((create_nullable_record<kind::int8>(200)), result[3]); // null becomes false with any condition
    }
}

TEST_F(sql_test, case_expression_null_for_condition) {
    // each condition of case expression is checked IS TRUE, i.e. null condition is treated as false
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (null)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select case c0 when 0 then 100 when 1 then 101 else 200 end from t0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(200)), result[0]);
    }
}

TEST_F(sql_test, coalesce_expression) {
    execute_statement("create table t0 (c0 int, c1 int, c2 int)");
    execute_statement("INSERT INTO t0 VALUES (null, null, 10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select coalesce(c0, c1, c2) from t0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), result[0]);
    }
}

TEST_F(sql_test, coalesce_expression_all_null) {
    execute_statement("create table t0 (c0 int, c1 int, c2 int)");
    execute_statement("INSERT INTO t0 VALUES (null, null, null)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select coalesce(c0, c1, c2) from t0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>({-1}, {true})), result[0]);
    }
}

TEST_F(sql_test, coalesce_expression_with_type_conversion) {
    execute_statement("create table t0 (c0 double, c1 decimal(5), c2 int)");
    execute_statement("INSERT INTO t0 VALUES (null, null, 10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select coalesce(c0, c1, c2) from t0", result);
        ASSERT_EQ(1, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::float8>(10)), result[0]);
    }
}

TEST_F(sql_test, nullif_expression) {
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1), (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select nullif(c0, 2) from t0", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4>({-1}, {true})), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[1]);
    }
    {
        // comparing with null is always false
        std::vector<mock::basic_record> result{};
        execute_query("select nullif(c0, null) from t0", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[1]);
    }
    {
        // it's possible to compare null with null - compiler once made it as an error
        std::vector<mock::basic_record> result{};
        execute_query("select nullif(null, null) from t0", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::unknown>(-1, {true})), result[0]);
        EXPECT_EQ((create_nullable_record<kind::unknown>(-1, {true})), result[1]);
    }
}

TEST_F(sql_test, null_comparison_by_operator) {
    // compare literal-like NULL with itself - once compiler made it as an error
    execute_statement("create table t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1), (NULL)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t0 where NULL = NULL", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t0 where NULL <> NULL", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t0 where c0 = NULL", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t0 where c0 <> NULL", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t0 where NULL IS NULL", result);
        ASSERT_EQ(2, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4>(-1, {true})), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[1]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from t0 where NULL IS NOT NULL", result);
        ASSERT_EQ(0, result.size());
    }
}


TEST_F(sql_test, type_of_cast_conversion) {
    // verify the result type of cast - issue #982
    // TODO currently comparison with basic_record does not distinguish triples of same value with different representation
    execute_statement("create table t (c0 decimal(5,2))");
    execute_statement("INSERT INTO t VALUES (1.00)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select c0 from t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((typed_nullable_record<kind::decimal>(std::tuple{meta::decimal_type(5,2)}, std::forward_as_tuple(triple{1, 0, 100, -2}))), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select cast(c0 as decimal(5,2)) from t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((typed_nullable_record<kind::decimal>(std::tuple{meta::decimal_type(5,2)}, std::forward_as_tuple(triple{1, 0, 1, 0}))), result[0]);
    }
}

TEST_F(sql_test, conditional_and_or) {
    // regression test for issue #1012
    execute_statement("create table t (c0 int, c1 int)");
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    {
        // unknown or true
        std::vector<mock::basic_record> result{};
        execute_query("select c0 from t where c0 < c1 or c1 is null", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        // unknown and false
        std::vector<mock::basic_record> result{};
        execute_query("select c0 from t where c0 < c1 and c1 is not null", result);
        ASSERT_EQ(0, result.size());
    }
}

}
