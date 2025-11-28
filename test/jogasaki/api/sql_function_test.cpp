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
#include <jogasaki/executor/tables.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/add_test_tables.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_function_test :
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
        utils::add_test_tables();
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(sql_function_test, count_empty_records) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_function_test, count_empty_records_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_function_test, sum_empty_records) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_function_test, sum_empty_records_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_function_test, concat) {
    execute_statement("CREATE TABLE T (C0 VARCHAR(10), C1 VARCHAR(10))");
    execute_statement("INSERT INTO T VALUES ('AAA', 'BBB')");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 || C1 FROM T", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((typed_nullable_record<kind::character>(std::tuple{meta::field_type{std::make_shared<meta::character_field_option>(true, 20)}}, {accessor::text{"AAABBB"}})), result[0]);
}

// LENGTH not registered yet
TEST_F(sql_function_test, DISABLED_strlen) {
    execute_statement("CREATE TABLE T (C0 CHAR(10), C1 VARCHAR(10))");
    execute_statement("INSERT INTO T VALUES ('AAA', 'BBB')");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT LENGTH(C0), LENGTH(C1) FROM T", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(10, 3)), result[0]);
}

TEST_F(sql_function_test, remainder) {
    execute_statement("CREATE TABLE T (C0 INT, C1 INT)");
    execute_statement("INSERT INTO T VALUES (9, 4)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 % C1 FROM T", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
}

TEST_F(sql_function_test, count_null) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_function_test, sum_null) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

// SUM is not available for distinct yet
TEST_F(sql_function_test, DISABLED_sum_distinct) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(30.0, rec.get_value<double>(0));
}
TEST_F(sql_function_test, count_all) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(all C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(3, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_function_test, count_distinct) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(2, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_function_test, count_distinct_empty) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_function_test, count_distinct_null) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_function_test, count_rows) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(*) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(2, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_function_test, max) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C0), MAX(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_FALSE(rec.is_null(1));
    EXPECT_EQ(3, rec.get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(30.0, rec.get_value<double>(1));
}

TEST_F(sql_function_test, min) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MIN(C0), MIN(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_FALSE(rec.is_null(1));
    EXPECT_EQ(1, rec.get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(10.0, rec.get_value<double>(1));
}

TEST_F(sql_function_test, count_rows_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(*) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_function_test, count_rows_empty_table_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(*) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_function_test, avg_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT AVG(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_function_test, avg_empty_table_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT AVG(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_function_test, max_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_function_test, max_empty_table_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_function_test, min_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MIN(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_function_test, min_empty_table_with_grouping) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MIN(C1) FROM T0 GROUP BY C1", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_function_test, aggregate_decimals) {
    execute_statement("CREATE TABLE TT(C0 DECIMAL(5,3) NOT NULL PRIMARY KEY)");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal}
    };
    auto ps = api::create_parameter_set();
    auto v10 = decimal_v{1, 0, 10, 0}; // 10
    auto v20 = decimal_v{1, 0, 20, 0}; // 20
    ps->set_decimal("p0", v10);
    ps->set_decimal("p1", v20);
    execute_statement("INSERT INTO TT (C0) VALUES (:p0)", variables, *ps);
    execute_statement("INSERT INTO TT (C0) VALUES (:p1)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C0), MIN(C0), COUNT(C0), AVG(C0) FROM TT", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_FALSE(rec.is_null(1));
    EXPECT_FALSE(rec.is_null(2));
    EXPECT_FALSE(rec.is_null(3));
    auto v15 = decimal_v{1, 0, 15, 0}; // 15

    auto dec = meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto i64 = meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>};
    EXPECT_EQ((mock::typed_nullable_record<
        kind::decimal, kind::decimal, kind::int8, kind::decimal
    >(
        std::tuple{
            dec, dec, i64, dec
        },
        {
            v20, v10, 2, v15
        }
    )), result[0]);
}

TEST_F(sql_function_test, aggregate_decimals_scale_zero) {
    // regression testcase with issue #782 where aggregate with DECIMAL(5) caused problems
    execute_statement("CREATE TABLE TT(C0 DECIMAL(5,0) NOT NULL PRIMARY KEY)");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal}
    };
    auto ps = api::create_parameter_set();
    auto v10 = decimal_v{1, 0, 10, 0}; // 10
    auto v20 = decimal_v{1, 0, 20, 0}; // 20
    ps->set_decimal("p0", v10);
    ps->set_decimal("p1", v20);
    execute_statement("INSERT INTO TT (C0) VALUES (:p0)", variables, *ps);
    execute_statement("INSERT INTO TT (C0) VALUES (:p1)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C0), MIN(C0), COUNT(C0), AVG(C0) FROM TT", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_FALSE(rec.is_null(1));
    EXPECT_FALSE(rec.is_null(2));
    EXPECT_FALSE(rec.is_null(3));
    auto v15 = decimal_v{1, 0, 15, 0}; // 15

    auto dec = meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto i64 = meta::field_type{meta::field_enum_tag<meta::field_type_kind::int8>};
    EXPECT_EQ((mock::typed_nullable_record<
        kind::decimal, kind::decimal, kind::int8, kind::decimal
    >(
        std::tuple{
            dec, dec, i64, dec
        },
        {
            v20, v10, 2, v15
        }
    )), result[0]);
}

TEST_F(sql_function_test, min_max_date) {
    execute_statement("CREATE TABLE t (c0 DATE NOT NULL PRIMARY KEY)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::date},
    };
    auto dt0 = date{2000, 1, 1};
    auto dt1 = date{2000, 1, 2};
    auto dt2 = date{2001, 1, 1};
    {
        auto ps = api::create_parameter_set();
        ps->set_date("p0", dt0);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_date("p0", dt1);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_date("p0", dt2);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MIN(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::date>(std::tuple{date_type()}, { dt0 })), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MAX(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::date>(std::tuple{date_type()}, { dt2 })), result[0]);
    }
}

TEST_F(sql_function_test, min_max_time) {
    execute_statement("CREATE TABLE t (c0 TIME NOT NULL PRIMARY KEY)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::time_of_day},
    };
    auto td0 = time_of_day{12, 0, 0};
    auto td1 = time_of_day{12, 0, 1};
    auto td2 = time_of_day{12, 1, 0};
    {
        auto ps = api::create_parameter_set();
        ps->set_time_of_day("p0", td0);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_of_day("p0", td1);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_of_day("p0", td2);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MIN(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::time_of_day>(std::tuple{time_of_day_type(false)}, { td0 })), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MAX(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::time_of_day>(std::tuple{time_of_day_type(false)}, { td2 })), result[0]);
    }
}

TEST_F(sql_function_test, min_max_time_with_tz) {
    execute_statement("CREATE TABLE t (c0 TIME WITH TIME ZONE NOT NULL PRIMARY KEY)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::time_of_day_with_time_zone},
    };
    auto td0 = time_of_day{12, 0, 0};
    auto td1 = time_of_day{12, 0, 1};
    auto td2 = time_of_day{12, 1, 0};
    {
        auto ps = api::create_parameter_set();
        ps->set_time_of_day("p0", td0);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_of_day("p0", td1);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_of_day("p0", td2);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MIN(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::time_of_day>(std::tuple{time_of_day_type(true)}, { td0 })), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MAX(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::time_of_day>(std::tuple{time_of_day_type(true)}, { td2 })), result[0]);
    }
}

TEST_F(sql_function_test, min_max_timestamp) {
    execute_statement("CREATE TABLE t (c0 TIMESTAMP NOT NULL PRIMARY KEY)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::time_point},
    };
    auto tp0 = time_point{date{2000, 1, 1}, time_of_day{12, 0, 0}};
    auto tp1 = time_point{date{2000, 1, 1}, time_of_day{12, 0, 1}};
    auto tp2 = time_point{date{2000, 1, 2}, time_of_day{12, 0, 0}};
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tp0);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tp1);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tp2);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MIN(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
        EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(std::tuple{tp}, { tp0 })), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MAX(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
        EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(std::tuple{tp}, { tp2 })), result[0]);
    }
}

TEST_F(sql_function_test, min_max_timestamp_with_tz) {
    execute_statement("CREATE TABLE t (c0 TIMESTAMP WITH TIME ZONE NOT NULL PRIMARY KEY)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::time_point_with_time_zone},
    };
    auto tp0 = time_point{date{2000, 1, 1}, time_of_day{12, 0, 0}};
    auto tp1 = time_point{date{2000, 1, 1}, time_of_day{12, 0, 1}};
    auto tp2 = time_point{date{2000, 1, 2}, time_of_day{12, 0, 0}};
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tp0);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tp1);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tp2);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MIN(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(true)};
        EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(std::tuple{tp}, { tp0 })), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MAX(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(true)};
        EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(std::tuple{tp}, { tp2 })), result[0]);
    }
}

TEST_F(sql_function_test, min_max_timestamp_negative) {
    execute_statement("CREATE TABLE t (c0 TIMESTAMP NOT NULL PRIMARY KEY)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::time_point},
    };
    auto tpn2 = time_point{date{1969, 12, 31}, time_of_day{23, 59, 59, time_of_day::time_unit{999999998}}};
    auto tpn1 = time_point{date{1969, 12, 31}, time_of_day{23, 59, 59, time_of_day::time_unit{999999999}}};
    auto tp0 = time_point{date{1970, 1, 1}, time_of_day{0, 0, 0, time_of_day::time_unit{0}}};
    auto tpp1 = time_point{date{1970, 1, 1}, time_of_day{0, 0, 0, time_of_day::time_unit{1}}};
    auto tpp2 = time_point{date{1970, 1, 1}, time_of_day{0, 0, 0, time_of_day::time_unit{2}}};
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tpn2);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tpn1);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tp0);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tpp1);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("p0", tpp2);
        execute_statement( "INSERT INTO t VALUES (:p0)", variables, *ps);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MIN(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
        EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(std::tuple{tp}, { tpn2 })), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT MAX(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
        EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(std::tuple{tp}, { tpp2 })), result[0]);
    }
}

TEST_F(sql_function_test, verify_parameter_application_conversion) {
    // no count(char) is registered, but count(varchar) is applied instead for char columns
    // by paramater application conversion
    execute_statement("create table t (c0 char(3))");
    execute_statement("insert into t values ('aaa'), ('bbb'), ('ccc')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT COUNT(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(3)), result[0]);
    }
}

TEST_F(sql_function_test, count_distinct_varlen) {
    // regression testcase for issue 946
    execute_statement("create table t (c0 char(20))");
    execute_statement("insert into t values ('a'), ('a'), ('b')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT COUNT(distinct c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(2)), result[0]);
    }
}

}  // namespace jogasaki::testing
