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

using decimal_v = takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class function_mod_test :
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

struct TestCase {
    std::optional<std::string> dividend;
    std::optional<std::string> divisor;
    std::optional<int> expected;
};
struct TestCase_Decimal {
    std::optional<std::string> dividend;
    std::optional<std::string> divisor;
    std::int8_t sigh;
    std::uint64_t high;
    std::uint64_t low;
    std::int32_t exponent;
};

using namespace std::string_view_literals;


TEST_F(function_mod_test, int_int) {
    execute_statement("create table t (c0 INT)");
    execute_statement("insert into t values (-8)");
    std::vector<TestCase> test_cases = {
        // basic
        {"-10::INT", "3::INT", -1}, {"10::INT", "3::INT", 1},
        {"10::INT", "-3::INT", 1}, {"-10::INT", "-3::INT", -1},
        {"-3::INT", "21113333::INT",-3},{"3::INT", "21113333::INT", 3},
        {"-3::INT", "-21113333::INT",-3},{"3::INT", "-21113333::INT", 3},
        // dividend is zero
        {"0::INT", "3::INT", 0}, {"0::INT", "-3::INT", 0},
        // dividend is INT32_MAX,INT32_MIN
        {"2147483647::INT", "3::INT", 1}, {"-2147483648::INT", "3::INT", -1},
        {"2147483647::INT", "-3::INT", 1}, {"-2147483648::INT", "-3::INT", -1},
        // divisor is INT32_MAX,INT32_MIN
        {"-10::INT", "2147483647::INT", -10}, {"10::INT", "2147483647::INT", 10},
        {"10::INT", "-2147483648::INT", 10}, {"-10::INT", "-2147483648::INT", -10},
        // dividend is INT32_MAX,INT32_MIN  and divisor is INT32_MAX,INT32_MIN
        //                                          -2147483648::INT%2147483647::INT -> 0
        // 2147483647::INT%-2147483648::INT -> 0
        {"2147483647::INT", "2147483647::INT", 0}, {"-2147483648::INT", "2147483647::INT", 0},
        {"2147483647::INT", "-2147483648::INT", 0}, {"-2147483648::INT", "-2147483648::INT", 0}
    };
    for (const auto& test : test_cases) {
        std::vector<mock::basic_record> result{};
        if (test.dividend.has_value() && test.divisor.has_value() && test.expected.has_value()) {
            std::string query = std::string("SELECT mod(") + test.dividend.value() +
                                std::string(",") + test.divisor.value() + std::string(") FROM t");
            execute_query(query, result);
            ASSERT_EQ(1, result.size()) << "Query failed: " << query;
            EXPECT_EQ(create_nullable_record<kind::int4>(test.expected.value()), result[0])
                << "Failed query: " << query;
        }
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,2::INT) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(2::INT,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        test_stmt_err("SELECT mod(2::INT,0::INT) FROM t", error_code::value_evaluation_exception);
    }
    {
        test_stmt_err("SELECT mod(0::INT,0::INT) FROM t", error_code::value_evaluation_exception);
    }
}

TEST_F(function_mod_test, int_bigint) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (-8)");
    std::vector<TestCase> test_cases = {
        // basic
        {"-10::INT", "3::BIGINT", -1}, {"10::INT", "3::BIGINT", 1},
        {"10::INT", "-3::BIGINT", 1}, {"-10::INT", "-3::BIGINT", -1},
        {"-3::INT", "21113333::BIGINT",-3},{"3::INT", "21113333::BIGINT", 3},
        {"-3::INT", "-21113333::BIGINT",-3},{"3::INT", "-21113333::BIGINT", 3},
        // dividend is zero
        {"0::INT", "3::BIGINT", 0}, {"0::INT", "-3::BIGINT", 0},
        // dividend is INT32_MAX,INT32_MIN
        {"2147483647::INT", "3::BIGINT", 1},
        {"-2147483648::INT", "3::BIGINT", -1},
        {"2147483647::INT", "-3::BIGINT", 1},
        {"-2147483648::INT", "-3::BIGINT", -1},
        // divisor is INT64_MAX, INT64_MIN
        {"-10::INT", "9223372036854775807::BIGINT", -10},
        {"10::INT", "9223372036854775807::BIGINT", 10},
        {"10::INT", "-9223372036854775808::BIGINT", 10},
        {"-10::INT", "-9223372036854775808::BIGINT", -10},
        // dividend is INT32_MAX, INT32_MIN and divisor is INT64_MAX, INT64_MIN
        {"2147483647::INT", "9223372036854775807::BIGINT", 2147483647},
        {"-2147483648::INT", "9223372036854775807::BIGINT", -2147483647},
        {"2147483647::INT", "-9223372036854775808::BIGINT", 2147483647},
        {"-2147483648::INT", "-9223372036854775808::BIGINT", -2147483647}
    };
    for (const auto& test : test_cases) {
        std::vector<mock::basic_record> result{};
        if (test.dividend.has_value() && test.divisor.has_value() && test.expected.has_value()) {
            std::string query = std::string("SELECT mod(") + test.dividend.value() +
                                std::string(",") + test.divisor.value() + std::string(") FROM t");
            execute_query(query, result);
            ASSERT_EQ(1, result.size()) << "Query failed: " << query;
            EXPECT_EQ(create_nullable_record<kind::int8>(test.expected.value()), result[0])
                << "Failed query: " << query;
        }
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,2::BIGINT) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(2::INT,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        test_stmt_err("SELECT mod(2::INT,0::BIGINT) FROM t", error_code::value_evaluation_exception);
    }
    {
        test_stmt_err("SELECT mod(0::INT,0::BIGINT) FROM t", error_code::value_evaluation_exception);
    }
}

TEST_F(function_mod_test, bigint_int) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (-8)");
    std::vector<TestCase> test_cases = {
        // basic
        {"-10::BIGINT", "3::INT", -1}, {"10::BIGINT", "3::INT", 1},
        {"10::BIGINT", "-3::INT", 1}, {"-10::BIGINT", "-3::INT", -1},
        {"-3::BIGINT", "21113333::INT",-3},{"3::BIGINT", "21113333::INT", 3},
        {"-3::BIGINT", "-21113333::INT",-3},{"3::BIGINT", "-21113333::INT", 3},
        // dividend is zero
        {"0::BIGINT", "3::INT", 0}, {"0::BIGINT", "-3::INT", 0},
        // dividend is INT64_MAX,INT64_MIN
        {"9223372036854775807::BIGINT", "3::INT", 1},
        {"-9223372036854775808::BIGINT", "3::INT", -1},
        {"9223372036854775807::BIGINT", "-3::INT", 1},
        {"-9223372036854775808::BIGINT", "-3::INT", -1},
        // divisor is INT64_MAX, INT64_MIN
        {"-10::BIGINT", "2147483647::INT", -10},
        {"10::BIGINT", "2147483647::INT", 10},
        {"10::BIGINT", "-2147483648::INT", 10},
        {"-10::BIGINT", "-2147483648::INT", -10},
        // dividend is INT64_MAX, INT64_MIN and divisor is INT64_MAX, INT64_MIN
        {"9223372036854775807::BIGINT", "2147483647::INT", 1},
        {"-9223372036854775808::BIGINT", "2147483647::INT",-1},
        {"9223372036854775807::BIGINT", "-2147483648::INT", 1},
        {"-9223372036854775808::BIGINT", "-2147483648::INT", -1}
    };
    for (const auto& test : test_cases) {
        std::vector<mock::basic_record> result{};
        if (test.dividend.has_value() && test.divisor.has_value() && test.expected.has_value()) {
            std::string query = std::string("SELECT mod(") + test.dividend.value() +
                                std::string(",") + test.divisor.value() + std::string(") FROM t");
            execute_query(query, result);
            ASSERT_EQ(1, result.size()) << "Query failed: " << query;
            EXPECT_EQ(create_nullable_record<kind::int8>(test.expected.value()), result[0])
                << "Failed query: " << query;
        }
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,2::INT) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(2::BIGINT,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        test_stmt_err("SELECT mod(2::BIGINT,0::INT) FROM t", error_code::value_evaluation_exception);
    }
    {
        test_stmt_err("SELECT mod(0::BIGINT,0::INT) FROM t", error_code::value_evaluation_exception);
    }
}

TEST_F(function_mod_test, bigint_bigint) {
    execute_statement("create table t (c0 BIGINT)");
    execute_statement("insert into t values (-8)");
    std::vector<TestCase> test_cases = {
        // basic
        {"-10::BIGINT", "3::BIGINT", -1}, {"10::BIGINT", "3::BIGINT", 1},
        {"10::BIGINT", "-3::BIGINT", 1}, {"-10::BIGINT", "-3::BIGINT", -1},
        {"-3::BIGINT", "21113333::BIGINT",-3},{"3::BIGINT", "21113333::BIGINT", 3},
        {"-3::BIGINT", "-21113333::BIGINT",-3},{"3::BIGINT", "-21113333::BIGINT", 3},
        // dividend is zero
        {"0::BIGINT", "3::BIGINT", 0}, {"0::BIGINT", "-3::BIGINT", 0},
        // dividend is INT64_MAX,INT64_MIN
        {"9223372036854775807::BIGINT", "3::BIGINT", 1},
        {"-9223372036854775808::BIGINT", "3::BIGINT", -1},
        {"9223372036854775807::BIGINT", "-3::BIGINT", 1},
        {"-9223372036854775808::BIGINT", "-3::BIGINT", -1},
        // divisor is INT64_MAX, INT64_MIN
        {"-10::BIGINT", "9223372036854775807::BIGINT", -10},
        {"10::BIGINT", "9223372036854775807::BIGINT", 10},
        {"10::BIGINT", "-9223372036854775808::BIGINT", 10},
        {"-10::BIGINT", "-9223372036854775808::BIGINT", -10},
        // dividend is INT64_MAX, INT64_MIN and divisor is INT64_MAX, INT64_MIN
        // -9223372036854775808::BIGINT%9223372036854775807::BIGINT -> 0
        // 9223372036854775807::BIGINT%-9223372036854775808::BIGINT -> 0
        {"9223372036854775807::BIGINT", "9223372036854775807::BIGINT", 0},
        {"-9223372036854775808::BIGINT", "9223372036854775807::BIGINT", 0},
        {"9223372036854775807::BIGINT", "-9223372036854775808::BIGINT", 0},
        {"-9223372036854775808::BIGINT", "-9223372036854775808::BIGINT", 0}
    };
    for (const auto& test : test_cases) {
        std::vector<mock::basic_record> result{};
        if (test.dividend.has_value() && test.divisor.has_value() && test.expected.has_value()) {
            std::string query = std::string("SELECT mod(") + test.dividend.value() +
                                std::string(",") + test.divisor.value() + std::string(") FROM t");
            execute_query(query, result);
            ASSERT_EQ(1, result.size()) << "Query failed: " << query;
            EXPECT_EQ(create_nullable_record<kind::int8>(test.expected.value()), result[0])
                << "Failed query: " << query;
        }
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,2::BIGINT) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(2::BIGINT,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        test_stmt_err("SELECT mod(2::BIGINT,0::BIGINT) FROM t", error_code::value_evaluation_exception);
    }
    {
        test_stmt_err("SELECT mod(0::BIGINT,0::BIGINT) FROM t", error_code::value_evaluation_exception);
    }
}
TEST_F(function_mod_test, decimal) {
    execute_statement("create table t (c0 DECIMAL)");
    execute_statement("insert into t values (-8::DECIMAL)");
    auto fm =
        meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};

    std::vector<TestCase_Decimal> test_cases = {
        // basic
        {"5.5::DECIMAL(5,2)", "2::DECIMAL", 1, 0, 15, -1},    // 1.5
        {"5.5::DECIMAL(5,2)", "-2::DECIMAL", 1, 0, 15, -1},   // 1.5
        {"-5.5::DECIMAL(5,2)", "2::DECIMAL", -1, 0, 15, -1},  // -1.5
        {"-5.5::DECIMAL(5,2)", "-2::DECIMAL", -1, 0, 15, -1}, // -1.5

        {"5.5::DECIMAL(5,2)", "2::INT", 1, 0, 15, -1},    // 1.5
        {"5.5::DECIMAL(5,2)", "-2::INT", 1, 0, 15, -1},   // 1.5
        {"-5.5::DECIMAL(5,2)", "2::INT", -1, 0, 15, -1},  // -1.5
        {"-5.5::DECIMAL(5,2)", "-2::INT", -1, 0, 15, -1}, // -1.5

        {"5.5::DECIMAL(5,2)", "2::BIGINT", 1, 0, 15, -1},    // 1.5
        {"5.5::DECIMAL(5,2)", "-2::BIGINT", 1, 0, 15, -1},   // 1.5
        {"-5.5::DECIMAL(5,2)", "2::BIGINT", -1, 0, 15, -1},  // -1.5
        {"-5.5::DECIMAL(5,2)", "-2::BIGINT", -1, 0, 15, -1}, // -1.5

        {"76::INT", "33.3::DECIMAL(5,2)", 1, 0, 94, -1},       // 9.4
        {"76::INT", "-33.3::DECIMAL(5,2)", 1, 0, 94, -1},      // 9.4
        {"-76::INT", "33.3::DECIMAL(5,2)", -1, 0, 94, -1},     // -9.4
        {"-76::INT", "-33.3::DECIMAL(5,2)", -1, 0, 94, -1},    // -9.4
        {"76::BIGINT", "33.3::DECIMAL(5,2)", 1, 0, 94, -1},    // 9.4
        {"76::BIGINT", "-33.3::DECIMAL(5,2)", 1, 0, 94, -1},   // 9.4
        {"-76::BIGINT", "33.3::DECIMAL(5,2)", -1, 0, 94, -1},  // -9.4
        {"-76::BIGINT", "-33.3::DECIMAL(5,2)", -1, 0, 94, -1}, // -9.4

        {"4.55::DECIMAL(5,3)", "2.22::DECIMAL(5,3)", 1, 0, 11, -2},   // 0.11
        {"4.55::DECIMAL(5,3)", "-2.22::DECIMAL(5,3)", 1, 0, 11, -2},  // 0.11
        {"-4.55::DECIMAL(5,3)", "2.22::DECIMAL(5,3)", -1, 0, 11, -2}, //-0.11
        {"-4.55::DECIMAL(5,3)", "-2.22::DECIMAL(5,3)", -1, 0, 11, -2} //-0.11
    };

    for (const auto& test : test_cases) {
        std::vector<mock::basic_record> result{};
        if (test.dividend.has_value() && test.divisor.has_value()) {
            std::string query = std::string("SELECT mod(") + test.dividend.value() +
                                std::string(",") + test.divisor.value() + std::string(") FROM t");
            execute_query(query, result);
            auto r1 = mock::typed_nullable_record<kind::decimal>(
                std::tuple{fm}, {runtime_t<meta::field_type_kind::decimal>(
                                    test.sigh, test.high, test.low, test.exponent)});
            ASSERT_EQ(1, result.size()) << "Query failed: " << query;
            EXPECT_EQ(r1, result[0]) << "Failed query: " << query;
        }
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,2::DECIMAL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(2::DECIMAL,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT mod(NULL,NULL) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        test_stmt_err(
            "SELECT mod(2::DECIMAL,0::DECIMAL) FROM t", error_code::value_evaluation_exception);
    }
    {
        test_stmt_err(
            "SELECT mod(0::DECIMAL,0::DECIMAL) FROM t", error_code::value_evaluation_exception);
    }
}

} // namespace jogasaki::testing
