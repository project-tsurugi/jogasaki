/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>

#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace std::chrono_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;

using takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;

using kind = meta::field_type_kind;

/**
 * @brief tests for the EXTRACT family of scalar functions
 * @details the operand is always given as a column reference so that the functions are evaluated
 * at runtime. Evaluation on literal operands is covered by the compiler side tests.
 */
class function_extract_test:
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
        // compare decimal values strictly, i.e. including coefficient and exponent
        mock::basic_record::compare_decimals_as_triple_ = true;
    }

    void TearDown() override {
        mock::basic_record::compare_decimals_as_triple_ = false;  // reset global flag
        db_teardown();
    }

    /**
     * @brief create table s whose column c0 has the given type and store the given value in it
     * @details the value is passed as a literal to the insert statement, while the extract
     * functions under test always take the column c0 as their operand
     */
    void prepare(std::string_view coltype, std::string_view value) {
        execute_statement("create table s (pk int primary key, c0 "s + std::string{coltype} + ")");
        execute_statement("insert into s values (1, "s + std::string{value} + ")");
    }

    std::vector<mock::basic_record> run(std::string_view expr) {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT "s + std::string{expr} + " FROM s", result);
        return result;
    }

    /**
     * @brief create table s holding a single row whose datetime columns are all null
     * @details c0 is timestamp, c1 is date, c2 is time and c3 is timestamp with time zone
     */
    void prepare_all_null() {
        execute_statement(
            "create table s (pk int primary key, c0 timestamp, c1 date, c2 time, "
            "c3 timestamp with time zone)");
        execute_statement("insert into s (pk) values (1)");
    }

    void expect_null_int4(std::string_view expr) {
        auto result = run(expr);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(std::nullopt)), result[0]);
    }

    void expect_null_decimal(std::string_view expr) {
        auto result = run(expr);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
            std::tuple{decimal_type()}, std::nullopt)), result[0]);
    }

    void expect_null_date(std::string_view expr) {
        auto result = run(expr);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::date>(
            std::tuple{date_type()}, std::nullopt)), result[0]);
    }

    void expect_null_time_point(std::string_view expr, bool with_offset) {
        auto result = run(expr);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
            std::tuple{time_point_type(with_offset)}, std::nullopt)), result[0]);
    }
};

using namespace std::string_view_literals;

constexpr auto ts_type = "timestamp"sv;
constexpr auto tstz_type = "timestamp with time zone"sv;
constexpr auto date_type_name = "date"sv;
constexpr auto time_type_name = "time"sv;

///////////
// TIMESTAMP operand, first notation
///////////

TEST_F(function_extract_test, year_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(YEAR FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(2026)), result[0]);
}

TEST_F(function_extract_test, month_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(MONTH FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(7)), result[0]);
}

TEST_F(function_extract_test, day_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(DAY FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
}

TEST_F(function_extract_test, hour_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(HOUR FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(12)), result[0]);
}

TEST_F(function_extract_test, minute_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(MINUTE FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(34)), result[0]);
}

TEST_F(function_extract_test, second_from_timestamp_default_precision) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(SECOND FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
        std::tuple{decimal_type()}, triple{56'789'000'000, -9})), result[0]);
}

TEST_F(function_extract_test, second_from_timestamp_precision_3) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(SECOND(3) FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
        std::tuple{decimal_type()}, triple{56'789, -3})), result[0]);
}

TEST_F(function_extract_test, second_from_timestamp_precision_1_truncates) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(SECOND(1) FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
        std::tuple{decimal_type()}, triple{567, -1})), result[0]);
}

TEST_F(function_extract_test, second_from_timestamp_precision_0) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(SECOND(0) FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
        std::tuple{decimal_type()}, triple{56, 0})), result[0]);
}

TEST_F(function_extract_test, second_from_timestamp_precision_star) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(SECOND(*) FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
        std::tuple{decimal_type()}, triple{56'789'000'000, -9})), result[0]);
}

// the subsecond precision is clamped into the range [0, 9]
TEST_F(function_extract_test, second_from_timestamp_precision_over_max_is_clamped) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    // the largest size the parser accepts is 2^64-2, since 2^64-1 is reserved for the "*" notation
    for(auto&& n : {"10"sv, "100"sv, "2147483647"sv, "2147483648"sv,
        "9223372036854775808"sv, "18446744073709551614"sv}) {
        auto result = run("EXTRACT(SECOND("s + std::string{n} + ") FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
            std::tuple{decimal_type()}, triple{56'789'000'000, -9})), result[0]);
    }
}

// a precision beyond the parsable size range is a syntax error, rather than being silently
// resolved to zero - see issue #1514
TEST_F(function_extract_test, second_from_timestamp_precision_out_of_size_range_is_syntax_error) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    for(auto&& n : {"18446744073709551615"sv, "18446744073709551616"sv,
        "100000000000000000000"sv}) {
        test_stmt_err("SELECT EXTRACT(SECOND("s + std::string{n} + ") FROM c0) FROM s",
            error_code::syntax_exception);
    }
}

TEST_F(function_extract_test, year_to_second_from_timestamp_precision_out_of_size_range) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    // within the parsable range the precision is clamped, beyond it the statement is rejected
    {
        auto result = run("EXTRACT(YEAR TO SECOND(18446744073709551614) FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(std::tuple{time_point_type(false)},
                      time_point{date{2026, 7, 1}, time_of_day{12, 34, 56, 789ms}})),
            result[0]);
    }
    test_stmt_err("SELECT EXTRACT(YEAR TO SECOND(100000000000000000000) FROM c0) FROM s",
        error_code::syntax_exception);
}

///////////
// TIMESTAMP operand, second notation
///////////

TEST_F(function_extract_test, date_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(DATE FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::date>(
        std::tuple{date_type()}, date{2026, 7, 1})), result[0]);
}

// note: the bare DATE(<expr>) shortcut syntax is not accepted by the parser (DATE is a reserved
// word), so the "date" function is called via the delimited identifier
// (it is also verified via the EXTRACT(DATE FROM <expr>) notation above)

TEST_F(function_extract_test, date_shortcut_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("\"date\"(c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::date>(
        std::tuple{date_type()}, date{2026, 7, 1})), result[0]);
}

TEST_F(function_extract_test, date_shortcut_from_date) {
    prepare(date_type_name, "DATE'2026-07-21'");
    auto result = run("\"date\"(c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::date>(
        std::tuple{date_type()}, date{2026, 7, 21})), result[0]);
}

TEST_F(function_extract_test, date_shortcut_from_timestamptz) {
    global::config_pool()->zone_offset(9*60);
    // stored value is UTC 2025-12-31 23:00:00, local date is 2026-01-01
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2025-12-31 23:00:00Z'");
    auto result = run("\"date\"(c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::date>(
        std::tuple{date_type()}, date{2026, 1, 1})), result[0]);
}

TEST_F(function_extract_test, year_to_month_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-03-21 12:34:56.789'");
    auto result = run("EXTRACT(YEAR TO MONTH FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::date>(
        std::tuple{date_type()}, date{2026, 3, 1})), result[0]);
}

TEST_F(function_extract_test, year_to_day_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-03-21 12:34:56.789'");
    auto result = run("EXTRACT(YEAR TO DAY FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::date>(
        std::tuple{date_type()}, date{2026, 3, 21})), result[0]);
}

TEST_F(function_extract_test, year_to_hour_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(YEAR TO HOUR FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 7, 1}, time_of_day{12, 0, 0}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(false)}, exp)), result[0]);
}

TEST_F(function_extract_test, year_to_minute_from_timestamp) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789'");
    auto result = run("EXTRACT(YEAR TO MINUTE FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 7, 1}, time_of_day{12, 34, 0}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(false)}, exp)), result[0]);
}

TEST_F(function_extract_test, year_to_second_from_timestamp_default_precision) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789123456'");
    auto result = run("EXTRACT(YEAR TO SECOND FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 7, 1}, time_of_day{12, 34, 56, 789123456ns}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(false)}, exp)), result[0]);
}

TEST_F(function_extract_test, year_to_second_from_timestamp_precision_3) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789123456'");
    auto result = run("EXTRACT(YEAR TO SECOND(3) FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 7, 1}, time_of_day{12, 34, 56, 789000000ns}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(false)}, exp)), result[0]);
}

TEST_F(function_extract_test, year_to_second_from_timestamp_precision_0) {
    prepare(ts_type, "TIMESTAMP'2026-07-01 12:34:56.789123456'");
    auto result = run("EXTRACT(YEAR TO SECOND(0) FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 7, 1}, time_of_day{12, 34, 56}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(false)}, exp)), result[0]);
}

///////////
// TIMESTAMP WITH TIME ZONE operand, first notation
// internally the value is UTC and fields are computed in the system time zone
///////////

TEST_F(function_extract_test, fields_from_timestamptz) {
    global::config_pool()->zone_offset(9*60);
    // stored value is UTC 2026-07-01 03:34:56.789, local time is 2026-07-01 12:34:56.789
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56.789+09:00'");
    {
        auto result = run("EXTRACT(YEAR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2026)), result[0]);
    }
    {
        auto result = run("EXTRACT(MONTH FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(7)), result[0]);
    }
    {
        auto result = run("EXTRACT(DAY FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        auto result = run("EXTRACT(HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(12)), result[0]);
    }
    {
        auto result = run("EXTRACT(MINUTE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(34)), result[0]);
    }
    {
        auto result = run("EXTRACT(SECOND(3) FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
            std::tuple{decimal_type()}, triple{56'789, -3})), result[0]);
    }
}

TEST_F(function_extract_test, fields_from_timestamptz_crossing_day_boundary) {
    global::config_pool()->zone_offset(9*60);
    // stored value is UTC 2025-12-31 23:59:59, local time is 2026-01-01 08:59:59
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2025-12-31 23:59:59Z'");
    {
        auto result = run("EXTRACT(YEAR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2026)), result[0]);
    }
    {
        auto result = run("EXTRACT(MONTH FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        auto result = run("EXTRACT(DAY FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    {
        auto result = run("EXTRACT(HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(8)), result[0]);
    }
}

TEST_F(function_extract_test, fields_from_timestamptz_literal_offset_differs_from_system_zone) {
    global::config_pool()->zone_offset(9*60);
    // the offset in the literal only determines the stored UTC value, while the fields are
    // computed in the system time zone
    // stored value is UTC 2026-07-01 17:34:56, local time is 2026-07-02 02:34:56
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56-05:00'");
    {
        auto result = run("EXTRACT(DAY FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
    {
        auto result = run("EXTRACT(HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
    {
        auto result = run("EXTRACT(MINUTE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(34)), result[0]);
    }
}

TEST_F(function_extract_test, timezone_hour_and_minute) {
    global::config_pool()->zone_offset(9*60);
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56Z'");
    {
        auto result = run("EXTRACT(TIMEZONE_HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(9)), result[0]);
    }
    {
        auto result = run("EXTRACT(TIMEZONE_MINUTE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
    }
}

TEST_F(function_extract_test, timezone_hour_and_minute_negative_offset) {
    global::config_pool()->zone_offset(-(9*60+30));
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56Z'");
    {
        auto result = run("EXTRACT(TIMEZONE_HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(-9)), result[0]);
    }
    {
        auto result = run("EXTRACT(TIMEZONE_MINUTE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(-30)), result[0]);
    }
}

TEST_F(function_extract_test, timezone_hour_and_minute_sub_hour_negative_offset) {
    // the hour part is zero and only the minute part carries the sign of the offset
    global::config_pool()->zone_offset(-30);
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56Z'");
    {
        auto result = run("EXTRACT(TIMEZONE_HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
    }
    {
        auto result = run("EXTRACT(TIMEZONE_MINUTE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(-30)), result[0]);
    }
}

TEST_F(function_extract_test, timezone_hour_and_minute_sub_hour_positive_offset) {
    global::config_pool()->zone_offset(30);
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56Z'");
    {
        auto result = run("EXTRACT(TIMEZONE_HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
    }
    {
        auto result = run("EXTRACT(TIMEZONE_MINUTE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(30)), result[0]);
    }
}

TEST_F(function_extract_test, timezone_hour_and_minute_positive_offset_with_minutes) {
    // both parts have the same sign
    global::config_pool()->zone_offset(5*60+45);
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56Z'");
    {
        auto result = run("EXTRACT(TIMEZONE_HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(5)), result[0]);
    }
    {
        auto result = run("EXTRACT(TIMEZONE_MINUTE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(45)), result[0]);
    }
}

TEST_F(function_extract_test, timezone_hour_and_minute_zero_offset) {
    global::config_pool()->zone_offset(0);
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56Z'");
    {
        auto result = run("EXTRACT(TIMEZONE_HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
    }
    {
        auto result = run("EXTRACT(TIMEZONE_MINUTE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(0)), result[0]);
    }
}

///////////
// TIMESTAMP WITH TIME ZONE operand, second notation
///////////

TEST_F(function_extract_test, date_from_timestamptz_crossing_day_boundary) {
    global::config_pool()->zone_offset(9*60);
    // stored value is UTC 2025-12-31 23:00:00, local date is 2026-01-01
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2025-12-31 23:00:00Z'");
    auto result = run("EXTRACT(DATE FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::date>(
        std::tuple{date_type()}, date{2026, 1, 1})), result[0]);
}

TEST_F(function_extract_test, year_to_month_from_timestamptz) {
    global::config_pool()->zone_offset(9*60);
    // stored value is UTC 2026-01-14 23:00:00, local date is 2026-01-15,
    // then truncated to the first day of the month
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-01-14 23:00:00Z'");
    auto result = run("EXTRACT(YEAR TO MONTH FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::date>(
        std::tuple{date_type()}, date{2026, 1, 1})), result[0]);
}

TEST_F(function_extract_test, year_to_day_from_timestamptz) {
    global::config_pool()->zone_offset(9*60);
    // stored value is UTC 2025-12-31 23:00:00, local date is 2026-01-01
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2025-12-31 23:00:00Z'");
    auto result = run("EXTRACT(YEAR TO DAY FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::date>(
        std::tuple{date_type()}, date{2026, 1, 1})), result[0]);
}

TEST_F(function_extract_test, year_to_hour_from_timestamptz) {
    global::config_pool()->zone_offset(9*60);
    // stored value is UTC 2026-07-01 03:34:56.789, local time is 2026-07-01 12:34:56.789
    // truncated in local time to 2026-07-01 12:00:00, that is UTC 2026-07-01 03:00:00
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56.789+09:00'");
    auto result = run("EXTRACT(YEAR TO HOUR FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 7, 1}, time_of_day{3, 0, 0}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(true)}, exp)), result[0]);
}

TEST_F(function_extract_test, year_to_hour_from_timestamptz_non_hour_boundary_offset) {
    // offset with minutes part affects the hour truncation result
    global::config_pool()->zone_offset(5*60+45);
    // stored value is UTC 2026-07-01 03:34:56, local time is 2026-07-01 09:19:56
    // truncated in local time to 2026-07-01 09:00:00, that is UTC 2026-07-01 03:15:00
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 03:34:56Z'");
    auto result = run("EXTRACT(YEAR TO HOUR FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 7, 1}, time_of_day{3, 15, 0}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(true)}, exp)), result[0]);
}

TEST_F(function_extract_test, year_to_hour_from_timestamptz_negative_offset_crossing_day_boundary) {
    global::config_pool()->zone_offset(-(5*60));
    // stored value is UTC 2026-01-01 02:30:00, local time is 2025-12-31 21:30:00
    // truncated in local time to 2025-12-31 21:00:00, that is UTC 2026-01-01 02:00:00
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-01-01 02:30:00Z'");
    auto result = run("EXTRACT(YEAR TO HOUR FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 1, 1}, time_of_day{2, 0, 0}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(true)}, exp)), result[0]);
}

TEST_F(function_extract_test, year_to_minute_from_timestamptz) {
    global::config_pool()->zone_offset(9*60);
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56.789+09:00'");
    auto result = run("EXTRACT(YEAR TO MINUTE FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 7, 1}, time_of_day{3, 34, 0}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(true)}, exp)), result[0]);
}

TEST_F(function_extract_test, year_to_second_from_timestamptz) {
    global::config_pool()->zone_offset(9*60);
    prepare(tstz_type, "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56.789+09:00'");
    auto result = run("EXTRACT(YEAR TO SECOND(1) FROM c0)");
    ASSERT_EQ(1, result.size());
    time_point exp{date{2026, 7, 1}, time_of_day{3, 34, 56, 700000000ns}};
    EXPECT_EQ((mock::typed_nullable_record<kind::time_point>(
        std::tuple{time_point_type(true)}, exp)), result[0]);
}

///////////
// DATE operand
///////////

TEST_F(function_extract_test, fields_from_date) {
    prepare(date_type_name, "DATE'2026-07-21'");
    {
        auto result = run("EXTRACT(YEAR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2026)), result[0]);
    }
    {
        auto result = run("EXTRACT(MONTH FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(7)), result[0]);
    }
    {
        auto result = run("EXTRACT(DAY FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(21)), result[0]);
    }
}

TEST_F(function_extract_test, date_ranges_from_date) {
    prepare(date_type_name, "DATE'2026-07-21'");
    {
        auto result = run("EXTRACT(DATE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::date>(
            std::tuple{date_type()}, date{2026, 7, 21})), result[0]);
    }
    {
        auto result = run("EXTRACT(YEAR TO MONTH FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::date>(
            std::tuple{date_type()}, date{2026, 7, 1})), result[0]);
    }
    {
        auto result = run("EXTRACT(YEAR TO DAY FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::date>(
            std::tuple{date_type()}, date{2026, 7, 21})), result[0]);
    }
}

TEST_F(function_extract_test, day_from_leap_day) {
    prepare(date_type_name, "DATE'2024-02-29'");
    auto result = run("EXTRACT(DAY FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(29)), result[0]);
}

TEST_F(function_extract_test, year_from_min_date) {
    prepare(date_type_name, "DATE'0001-01-01'");
    auto result = run("EXTRACT(YEAR FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
}

TEST_F(function_extract_test, year_from_max_date) {
    prepare(date_type_name, "DATE'9999-12-31'");
    auto result = run("EXTRACT(YEAR FROM c0)");
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(9999)), result[0]);
}

///////////
// TIME operand
///////////

TEST_F(function_extract_test, fields_from_time) {
    prepare(time_type_name, "TIME'12:34:56.789'");
    {
        auto result = run("EXTRACT(HOUR FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(12)), result[0]);
    }
    {
        auto result = run("EXTRACT(MINUTE FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(34)), result[0]);
    }
    {
        auto result = run("EXTRACT(SECOND FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
            std::tuple{decimal_type()}, triple{56'789'000'000, -9})), result[0]);
    }
    {
        auto result = run("EXTRACT(SECOND(3) FROM c0)");
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
            std::tuple{decimal_type()}, triple{56'789, -3})), result[0]);
    }
}

///////////
// multiple operand types in a single query
///////////

TEST_F(function_extract_test, columns) {
    global::config_pool()->zone_offset(9*60);
    execute_statement("create table s (c0 timestamp, c1 date, c2 time, c3 timestamp with time zone)");
    execute_statement(
        "insert into s values ("
        "TIMESTAMP'2026-07-01 12:34:56.789',"
        "DATE'2026-07-21',"
        "TIME'12:34:56.789',"
        "TIMESTAMP WITH TIME ZONE'2026-07-01 12:34:56.789+09:00')"
    );
    std::vector<mock::basic_record> result{};
    execute_query("SELECT EXTRACT(YEAR FROM c0), EXTRACT(DAY FROM c1), EXTRACT(MINUTE FROM c2), EXTRACT(HOUR FROM c3) FROM s", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int4>(2026, 21, 34, 12)), result[0]);
}

///////////
// invalid combination of field and operand type
///////////

TEST_F(function_extract_test, unsupported_field_for_operand_type_is_compile_error) {
    execute_statement("create table s (pk int primary key, c0 date)");
    // hour field is not extractable from date
    test_stmt_err("SELECT EXTRACT(HOUR FROM c0) FROM s", error_code::symbol_analyze_exception);
}

// the second notation accepts only timestamp and timestamp with time zone, except that
// DATE, YEAR TO MONTH and YEAR TO DAY additionally accept date
TEST_F(function_extract_test, time_of_day_range_field_from_date_is_compile_error) {
    execute_statement("create table s (pk int primary key, c0 date)");
    test_stmt_err(
        "SELECT EXTRACT(YEAR TO HOUR FROM c0) FROM s", error_code::symbol_analyze_exception);
    test_stmt_err(
        "SELECT EXTRACT(YEAR TO MINUTE FROM c0) FROM s", error_code::symbol_analyze_exception);
    test_stmt_err(
        "SELECT EXTRACT(YEAR TO SECOND FROM c0) FROM s", error_code::symbol_analyze_exception);
    test_stmt_err(
        "SELECT EXTRACT(YEAR TO SECOND(3) FROM c0) FROM s", error_code::symbol_analyze_exception);
}

TEST_F(function_extract_test, range_field_from_time_is_compile_error) {
    execute_statement("create table s (pk int primary key, c0 time)");
    test_stmt_err("SELECT EXTRACT(DATE FROM c0) FROM s", error_code::symbol_analyze_exception);
    test_stmt_err(
        "SELECT EXTRACT(YEAR TO MONTH FROM c0) FROM s", error_code::symbol_analyze_exception);
    test_stmt_err(
        "SELECT EXTRACT(YEAR TO DAY FROM c0) FROM s", error_code::symbol_analyze_exception);
    test_stmt_err(
        "SELECT EXTRACT(YEAR TO HOUR FROM c0) FROM s", error_code::symbol_analyze_exception);
    test_stmt_err(
        "SELECT EXTRACT(YEAR TO MINUTE FROM c0) FROM s", error_code::symbol_analyze_exception);
    test_stmt_err(
        "SELECT EXTRACT(YEAR TO SECOND FROM c0) FROM s", error_code::symbol_analyze_exception);
}

TEST_F(function_extract_test, date_shortcut_from_time_is_compile_error) {
    execute_statement("create table s (pk int primary key, c0 time)");
    test_stmt_err("SELECT \"date\"(c0) FROM s", error_code::symbol_analyze_exception);
}

TEST_F(function_extract_test, timezone_hour_from_plain_timestamp_is_compile_error) {
    execute_statement("create table s (pk int primary key, c0 timestamp)");
    // timezone_hour field is extractable only from timestamp with time zone
    test_stmt_err(
        "SELECT EXTRACT(TIMEZONE_HOUR FROM c0) FROM s",
        error_code::symbol_analyze_exception
    );
}

TEST_F(function_extract_test, timezone_minute_from_plain_timestamp_is_compile_error) {
    execute_statement("create table s (pk int primary key, c0 timestamp)");
    // timezone_minute field is extractable only from timestamp with time zone
    test_stmt_err(
        "SELECT EXTRACT(TIMEZONE_MINUTE FROM c0) FROM s",
        error_code::symbol_analyze_exception
    );
}

///////////
// null handling
///////////

// null operand results in null regardless of the function and the operand type
// (c0 is timestamp, c1 is date, c2 is time and c3 is timestamp with time zone, all null)

TEST_F(function_extract_test, null_input_year_month_day) {
    prepare_all_null();
    for(auto&& col : {"c0"sv, "c1"sv, "c3"sv}) {
        expect_null_int4("EXTRACT(YEAR FROM "s + std::string{col} + ")");
        expect_null_int4("EXTRACT(MONTH FROM "s + std::string{col} + ")");
        expect_null_int4("EXTRACT(DAY FROM "s + std::string{col} + ")");
    }
}

TEST_F(function_extract_test, null_input_hour_minute) {
    prepare_all_null();
    for(auto&& col : {"c0"sv, "c2"sv, "c3"sv}) {
        expect_null_int4("EXTRACT(HOUR FROM "s + std::string{col} + ")");
        expect_null_int4("EXTRACT(MINUTE FROM "s + std::string{col} + ")");
    }
}

TEST_F(function_extract_test, null_input_second) {
    prepare_all_null();
    for(auto&& col : {"c0"sv, "c2"sv, "c3"sv}) {
        expect_null_decimal("EXTRACT(SECOND FROM "s + std::string{col} + ")");
        expect_null_decimal("EXTRACT(SECOND(3) FROM "s + std::string{col} + ")");
        expect_null_decimal("EXTRACT(SECOND(*) FROM "s + std::string{col} + ")");
    }
}

TEST_F(function_extract_test, null_input_timezone_hour_and_minute) {
    prepare_all_null();
    expect_null_int4("EXTRACT(TIMEZONE_HOUR FROM c3)");
    expect_null_int4("EXTRACT(TIMEZONE_MINUTE FROM c3)");
}

TEST_F(function_extract_test, null_input_date_result) {
    prepare_all_null();
    for(auto&& col : {"c0"sv, "c1"sv, "c3"sv}) {
        expect_null_date("EXTRACT(DATE FROM "s + std::string{col} + ")");
        expect_null_date("\"date\"("s + std::string{col} + ")");
        expect_null_date("EXTRACT(YEAR TO MONTH FROM "s + std::string{col} + ")");
        expect_null_date("EXTRACT(YEAR TO DAY FROM "s + std::string{col} + ")");
    }
}

TEST_F(function_extract_test, null_input_time_point_result) {
    prepare_all_null();
    // c0 is timestamp and its result has no offset, while c3 is timestamp with time zone
    for(auto&& [col, with_offset] : {std::pair{"c0"sv, false}, std::pair{"c3"sv, true}}) {
        expect_null_time_point("EXTRACT(YEAR TO HOUR FROM "s + std::string{col} + ")", with_offset);
        expect_null_time_point("EXTRACT(YEAR TO MINUTE FROM "s + std::string{col} + ")", with_offset);
        expect_null_time_point("EXTRACT(YEAR TO SECOND FROM "s + std::string{col} + ")", with_offset);
        expect_null_time_point("EXTRACT(YEAR TO SECOND(3) FROM "s + std::string{col} + ")", with_offset);
    }
}

}  // namespace jogasaki::testing
