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

#include <boost/move/utility_core.hpp>
#include <cstddef>
#include <cstdint>
#include <gtest/gtest.h>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>

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

using decimal_v   = takatori::decimal::triple;
using date        = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point  = takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class function_round_test : public ::testing::Test, public api_test_base {

  public:
    // change this flag to debug with explain
    bool to_explain() override { return false; }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override { db_teardown(); }
};

struct TestCase {
    std::string scale;
    std::int64_t sign;
    std::uint64_t coefficient_high;
    std::uint64_t coefficient_low;
    std::int32_t exponent;
};

struct TestCaseDouble {
    std::string scale;
    double result;
};
struct TestCaseInt {
    std::string scale;
    int result;
};

using namespace std::string_view_literals;
TEST_F(function_round_test, int) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (-8)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::int4>(-8)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, maxint) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (2147483647)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::int4>(2147483647)), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, minint) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (-2147483648)");
    std::string query = std::string("SELECT round(c0) FROM t");

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::int4>(-2147483648)), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, bigint) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (-8)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::int8>(-8)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, maxbigint) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (9223372036854775807)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::int8>(9223372036854775807)), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, minbigint) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (-9223372036854775808)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::int8>(-9223372036854775808UL)), result[0])
        << "Failed query: " << query;
}

TEST_F(function_round_test, null) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (8)");
    std::string query = std::string("SELECT round(null) FROM t");
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
}

TEST_F(function_round_test, decimal_38_38_min) {
    execute_statement("create table t (c0 DECIMAL(38, 38))");
    execute_statement("insert into t values (-0.99999999999999999999999999999999999999)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    auto fm =
        meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto r1 = mock::typed_nullable_record<kind::decimal>(
        std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(-1, 0, 1, 0)});
    EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
}

TEST_F(function_round_test, decimal_38_38_max) {
    execute_statement("create table t (c0 DECIMAL(38, 38))");
    execute_statement("insert into t values (0.99999999999999999999999999999999999999)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    auto fm =
        meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto r1 = mock::typed_nullable_record<kind::decimal>(
        std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(
                            1, 5421010862427522170, 687399551400673280, -38)});
    EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
}

TEST_F(function_round_test, decimal_38_0_min) {
    execute_statement("create table t (c0 DECIMAL(38, 0))");
    execute_statement("insert into t values (-99999999999999999999999999999999999999)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    auto fm =
        meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto r1 = mock::typed_nullable_record<kind::decimal>(
        std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(
                            -1, 5421010862427522170, 687399551400673279, 0)});
    EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, decimal_38_0_max) {
    execute_statement("create table t (c0 DECIMAL(38, 0))");
    execute_statement("insert into t values (99999999999999999999999999999999999999)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    auto fm =
        meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto r1 = mock::typed_nullable_record<kind::decimal>(std::tuple{fm},
        {runtime_t<meta::field_type_kind::decimal>(1, 5421010862427522170, 687399551400673279, 0)});
    EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, decimal_1_1_min) {
    execute_statement("create table t (c0 DECIMAL(1, 1))");
    execute_statement("insert into t values (-0.9)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    auto fm =
        meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto r1 = mock::typed_nullable_record<kind::decimal>(
        std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(-1, 0, 1, 0)});
    EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, decimal_1_1_max) {
    execute_statement("create table t (c0 DECIMAL(1, 1))");
    execute_statement("insert into t values (0.9)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    auto fm =
        meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
    auto r1 = mock::typed_nullable_record<kind::decimal>(
        std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(1, 0, 10, -1)});
    EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, float_normal) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (-3.14159265358979323846)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float4>(-3.0)), result[0]) << "Failed query: " << query;
}

TEST_F(function_round_test, float_normal_plus) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (3.14)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float4>(3.00)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, float_normal_plus_half) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (3.5)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float4>(4.00)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, float_normal_plus_half_over) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (3.52)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float4>(4.00)), result[0]) << "Failed query: " << query;
}

TEST_F(function_round_test, float_minus_zero) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (-0.0)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float4>(0.0)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, float_minus_nan) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values ('-NaN')");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ(
        (create_nullable_record<kind::float4>(std::numeric_limits<float>::quiet_NaN())), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, float_nan) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values ('NaN')");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ(
        (create_nullable_record<kind::float4>(std::numeric_limits<float>::quiet_NaN())), result[0])
        << "Failed query: " << query;
}

TEST_F(function_round_test, float_infinity) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values ('Infinity')");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ(
        create_nullable_record<kind::float4>(std::numeric_limits<float>::infinity()), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, float_minus_infinity) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values ('-Infinity')");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ(
        create_nullable_record<kind::float4>(-std::numeric_limits<float>::infinity()), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, float_normal_minus) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (-3.14)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float4>(-3.00)), result[0])
        << "Failed query: " << query;
}

TEST_F(function_round_test, float_normal_minus_half) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (-3.5)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float4>(-4.00)), result[0])
        << "Failed query: " << query;
}

TEST_F(function_round_test, float_normal_minus_half_over) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (-3.533)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float4>(-4.000)), result[0])
        << "Failed query: " << query;
}

TEST_F(function_round_test, double_normal) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (-3.14159265358979323846)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float8>(-3.0)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, double_normal_plus) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (3.14)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float8>(3.00)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, double_normal_plus_half) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (3.5)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float8>(4.00)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, double_normal_plus_half_over) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (3.533)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float8>(4.00)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, double_minus_zero) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (-0.0)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float8>(0.0)), result[0]) << "Failed query: " << query;
}
TEST_F(function_round_test, double_minus_nan) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values ('-NaN')");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ(
        (create_nullable_record<kind::float8>(std::numeric_limits<double>::quiet_NaN())), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, double_nan) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values ('NaN')");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ(
        (create_nullable_record<kind::float8>(std::numeric_limits<double>::quiet_NaN())), result[0])
        << "Failed query: " << query;
}

TEST_F(function_round_test, double_infinity) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values ('Infinity')");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ(
        create_nullable_record<kind::float8>(std::numeric_limits<double>::infinity()), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, double_minus_infinity) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values ('-Infinity')");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ(
        create_nullable_record<kind::float8>(-std::numeric_limits<double>::infinity()), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, double_normal_minus) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (-3.14)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float8>(-3.00)), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, double_normal_minus_half) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (-3.5)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float8>(-4.00)), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, double_normal_minus_half_over) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (-3.533)");

    std::string query = std::string("SELECT round(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_EQ((create_nullable_record<kind::float8>(-4.00)), result[0])
        << "Failed query: " << query;
}
TEST_F(function_round_test, int_over) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (6666)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", 10000}, {"-3", 7000}, {"-2", 6700}, {"-1", 6670}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int4>(test.result)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_round_test, int_half) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (5555)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", 10000}, {"-3", 6000}, {"-2", 5600}, {"-1", 5560}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int4>(test.result)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_round_test, int_less) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (4444)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", 0}, {"-3", 4000}, {"-2", 4400}, {"-1", 4440}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int4>(test.result)), result[0])
            << "Failed query: " << query;
    }
}

TEST_F(function_round_test, int_over_minus) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (-6666)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", -10000}, {"-3", -7000}, {"-2", -6700}, {"-1", -6670}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int4>(test.result)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_round_test, int_half_minus) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (-5555)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", -10000}, {"-3", -6000}, {"-2", -5600}, {"-1", -5560}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int4>(test.result)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_round_test, int_less_minus) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (-4444)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", 0}, {"-3", -4000}, {"-2", -4400}, {"-1", -4440}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int4>(test.result)), result[0])
            << "Failed query: " << query;
    }
}

TEST_F(function_round_test, bigint_over) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (6666)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", 10000}, {"-3", 7000}, {"-2", 6700}, {"-1", 6670}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int8>(test.result)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_round_test, bigint_half) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (5555)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", 10000}, {"-3", 6000}, {"-2", 5600}, {"-1", 5560}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int8>(test.result)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_round_test, bigint_less) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (4444)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", 0}, {"-3", 4000}, {"-2", 4400}, {"-1", 4440}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int8>(test.result)), result[0])
            << "Failed query: " << query;
    }
}

TEST_F(function_round_test, bigint_over_minus) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (-6666)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", -10000}, {"-3", -7000}, {"-2", -6700}, {"-1", -6670}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int8>(test.result)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_round_test, bigint_half_minus) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (-5555)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", -10000}, {"-3", -6000}, {"-2", -5600}, {"-1", -5560}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int8>(test.result)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_round_test, bigint_less_minus) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (-4444)");
    std::vector<TestCaseInt> test_cases = {
        {"-5", 0}, {"-4", 0}, {"-3", -4000}, {"-2", -4400}, {"-1", -4440}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::int8>(test.result)), result[0])
            << "Failed query: " << query;
    }
}

TEST_F(function_round_test, float_normal_plus_half_over_minus_two) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (55555.55555)");
    std::vector<TestCaseDouble> test_cases = {{"2", 55555.56}, {"1", 55555.6}, {"0", 55556.00000},
        {"-1", 55560.00000}, {"-2", 55600.00000}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::float4>(test.result)), result[0])
            << "Failed query: " << query;
    }
}

TEST_F(function_round_test, double_normal_plus_half_over_minus_two) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (55555.55555)");
    std::vector<TestCaseDouble> test_cases = {{"2", 55555.56}, {"1", 55555.6}, {"0", 55556.00000},
        {"-1", 55560.00000}, {"-2", 55600.00000}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ((create_nullable_record<kind::float8>(test.result)), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_round_test, decimal_over) {
    execute_statement("create table t (c0 DECIMAL(6, 3))");
    execute_statement("insert into t values (666.666)");
    std::vector<TestCase> test_cases = {{"-4", 1, 0, 0, 0}, {"-3", 1, 0, 1000, 0},
        {"-2", 1, 0, 700, 0}, {"-1", 1, 0, 670, 0}, {"0", 1, 0, 667, 0}, {"1", 1, 0, 666700, -3},
        {"2", 1, 0, 666670, -3}, {"3", 1, 0, 666666, -3}, {"4", 1, 0, 666666, -3},
        {"5", 1, 0, 666666, -3}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        auto fm = meta::field_type{
            std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
        auto r1 = mock::typed_nullable_record<kind::decimal>(
            std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(test.sign,
                                test.coefficient_high, test.coefficient_low, test.exponent)});
        EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
    }
}
TEST_F(function_round_test, decimal_half) {
    execute_statement("create table t (c0 DECIMAL(6, 3))");
    execute_statement("insert into t values (555.555)");
    std::vector<TestCase> test_cases = {{"-4", 1, 0, 0, 0}, {"-3", 1, 0, 1000, 0},
        {"-2", 1, 0, 600, 0}, {"-1", 1, 0, 560, 0}, {"0", 1, 0, 556, 0}, {"1", 1, 0, 555600, -3},
        {"2", 1, 0, 555560, -3}, {"3", 1, 0, 555555, -3}, {"4", 1, 0, 555555, -3},
        {"5", 1, 0, 555555, -3}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        auto fm = meta::field_type{
            std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
        auto r1 = mock::typed_nullable_record<kind::decimal>(
            std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(test.sign,
                                test.coefficient_high, test.coefficient_low, test.exponent)});
        EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
    }
}
TEST_F(function_round_test, decimal_less) {
    execute_statement("create table t (c0 DECIMAL(6, 3))");
    execute_statement("insert into t values (444.444)");
    std::vector<TestCase> test_cases = {{"-4", 1, 0, 0, 0}, {"-3", 1, 0, 0, 0},
        {"-2", 1, 0, 400, 0}, {"-1", 1, 0, 440, 0}, {"0", 1, 0, 444, 0}, {"1", 1, 0, 444400, -3},
        {"2", 1, 0, 444440, -3}, {"3", 1, 0, 444444, -3}, {"4", 1, 0, 444444, -3}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        auto fm = meta::field_type{
            std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
        auto r1 = mock::typed_nullable_record<kind::decimal>(
            std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(test.sign,
                                test.coefficient_high, test.coefficient_low, test.exponent)});
        EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
    }
}
TEST_F(function_round_test, decimal_over_minus) {
    execute_statement("create table t (c0 DECIMAL(6, 3))");
    execute_statement("insert into t values (-666.666)");
    std::vector<TestCase> test_cases = {{"-4", 1, 0, 0, 0}, {"-3", -1, 0, 1000, 0},
        {"-2", -1, 0, 700, 0}, {"-1", -1, 0, 670, 0}, {"0", -1, 0, 667, 0},
        {"1", -1, 0, 666700, -3}, {"2", -1, 0, 666670, -3}, {"3", -1, 0, 666666, -3},
        {"4", -1, 0, 666666, -3}, {"5", -1, 0, 666666, -3}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        auto fm = meta::field_type{
            std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
        auto r1 = mock::typed_nullable_record<kind::decimal>(
            std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(test.sign,
                                test.coefficient_high, test.coefficient_low, test.exponent)});
        EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
    }
}
TEST_F(function_round_test, decimal_half_minus) {
    execute_statement("create table t (c0 DECIMAL(6, 3))");
    execute_statement("insert into t values (-555.555)");
    std::vector<TestCase> test_cases = {{"-4", 1, 0, 0, 0}, {"-3", -1, 0, 1000, 0},
        {"-2", -1, 0, 600, 0}, {"-1", -1, 0, 560, 0}, {"0", -1, 0, 556, 0},
        {"1", -1, 0, 555600, -3}, {"2", -1, 0, 555560, -3}, {"3", -1, 0, 555555, -3},
        {"4", -1, 0, 555555, -3}, {"5", -1, 0, 555555, -3}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        auto fm = meta::field_type{
            std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
        auto r1 = mock::typed_nullable_record<kind::decimal>(
            std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(test.sign,
                                test.coefficient_high, test.coefficient_low, test.exponent)});
        EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
    }
}
TEST_F(function_round_test, decimal_less_minus) {
    execute_statement("create table t (c0 DECIMAL(6, 3))");
    execute_statement("insert into t values (-444.444)");
    std::vector<TestCase> test_cases = {{"-4", 1, 0, 0, 0}, {"-3", 1, 0, 0, 0},
        {"-2", -1, 0, 400, 0}, {"-1", -1, 0, 440, 0}, {"0", -1, 0, 444, 0},
        {"1", -1, 0, 444400, -3}, {"2", -1, 0, 444440, -3}, {"3", -1, 0, 444444, -3},
        {"4", -1, 0, 444444, -3}};
    for (const auto& test : test_cases) {
        std::string query = std::string("SELECT round(c0,") + test.scale + std::string(") FROM t");
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        auto fm = meta::field_type{
            std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
        auto r1 = mock::typed_nullable_record<kind::decimal>(
            std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(test.sign,
                                test.coefficient_high, test.coefficient_low, test.exponent)});
        EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
    }
}
TEST_F(function_round_test, int_error) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (3)");
    {
        test_stmt_err(
            "SELECT round(c0,1) FROM t", error_code::unsupported_runtime_feature_exception);
    }
    {
        test_stmt_err(
            "SELECT round(c0,-10) FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_round_test, bigint_error) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (3)");
    {
        test_stmt_err(
            "SELECT round(c0,1) FROM t", error_code::unsupported_runtime_feature_exception);
    }
    {
        test_stmt_err(
            "SELECT round(c0,-19) FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_round_test, decimal_error) {
    execute_statement("create table t (c0 DECIMAL(2, 0))");
    execute_statement("insert into t values (-66)");
    {
        test_stmt_err(
            "SELECT round(c0,39) FROM t", error_code::unsupported_runtime_feature_exception);
    }
    {
        test_stmt_err(
            "SELECT round(c0,-39) FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_round_test, float_error) {
    execute_statement("create table t (c0 float)");
    execute_statement("insert into t values (3.14)");
    {
        test_stmt_err(
            "SELECT round(c0,8) FROM t", error_code::unsupported_runtime_feature_exception);
    }
    {
        test_stmt_err(
            "SELECT round(c0,-8) FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_round_test, double_error) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (-3.14159265358979323846)");
    {
        test_stmt_err(
            "SELECT round(c0,16) FROM t", error_code::unsupported_runtime_feature_exception);
    }
    {
        test_stmt_err(
            "SELECT round(c0,-16) FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_round_test, scale_null) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (-3.14159265358979323846)");
    std::vector<mock::basic_record> result{};
    std::string query = std::string("SELECT round(c0,NULL) FROM t");
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
}
TEST_F(function_round_test, value_null) {
    execute_statement("create table t (c0 double)");
    execute_statement("insert into t values (-3.14159265358979323846)");
    std::vector<mock::basic_record> result{};
    std::string query = std::string("SELECT round(NULL,3) FROM t");
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
}
} // namespace jogasaki::testing
