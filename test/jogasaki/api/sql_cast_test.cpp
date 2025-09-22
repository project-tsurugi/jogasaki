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

#include <cmath>
#include <cstddef>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/expr/details/constants.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

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

class sql_cast_test :
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
        mock::basic_record::compare_decimals_as_triple_ = false;  // reset global flag
    }
};

using namespace std::string_view_literals;

TEST_F(sql_cast_test, cast) {
    execute_statement("create table TT (C0 int primary key, C1 bigint, C2 float, C3 double)");
    execute_statement("INSERT INTO TT (C0, C1, C2, C3) VALUES (CAST('1' AS INT), CAST('10' AS BIGINT), CAST('100.0' AS FLOAT), CAST('1000.0' AS DOUBLE))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1, C2, C3 FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::float4, kind::float8>({1, 10, 100.0, 1000.0}, {false, false, false, false})), result[0]);
    }
}

TEST_F(sql_cast_test, cast_from_varchar) {
    execute_statement("create table TT (C0 varchar(10) primary key, C1 varchar(10), C2 varchar(10), C3 varchar(10))");
    execute_statement("INSERT INTO TT VALUES ('1', '10', '100.0', '1000.0')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS INT), CAST(C1 AS BIGINT), CAST(C2 AS REAL), CAST(C3 AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::float4, kind::float8>({1, 10, 100.0, 1000.0}, {false, false, false, false})), result[0]);
    }
}
TEST_F(sql_cast_test, cast_from_char) {
    // verify padding spaces are ignored
    execute_statement("create table TT (C0 char(10) primary key, C1 char(10), C2 char(10), C3 char(10))");
    execute_statement("INSERT INTO TT VALUES ('1', '10', '100.0', '1000.0')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS INT), CAST(C1 AS BIGINT), CAST(C2 AS REAL), CAST(C3 AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::float4, kind::float8>({1, 10, 100.0, 1000.0}, {false, false, false, false})), result[0]);
    }
}
TEST_F(sql_cast_test, cast_failure) {
    execute_statement("create table TT (C0 int primary key)");
    test_stmt_err("INSERT INTO TT (C0) VALUES (CAST('BADVALUE' AS INT))", error_code::value_evaluation_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement("INSERT INTO TT VALUES (1)");
    test_stmt_err("SELECT CAST('BADVALUE' AS INT) FROM TT", error_code::value_evaluation_exception);
}

TEST_F(sql_cast_test, cast_char_padding_truncation) {
    execute_statement("create table TT (C0 int primary key)");
    execute_statement("INSERT INTO TT VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST('ABCDEF' AS VARCHAR(5)), CAST('ABC' AS CHAR(5)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
           std::tuple{character_type(true, 5), character_type(false, 5)},
           std::forward_as_tuple(accessor::text{"ABCDE"}, accessor::text{"ABC  "}))), result[0]);
    }
}

TEST_F(sql_cast_test, insert_cast_to_char) {
    execute_statement("create table TT (C0 int primary key, C1 varchar(5), C2 char(5))");
    execute_statement("INSERT INTO TT VALUES (1, CAST('1' AS VARCHAR(5)), CAST('1' AS CHAR(5)))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C1, C2 FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
           std::tuple{character_type(true, 5), character_type(false, 5)},
           std::forward_as_tuple(accessor::text{"1"}, accessor::text{"1    "}))), result[0]);
    }
}

TEST_F(sql_cast_test, select_cast_to_char) {
    execute_statement("create table TT (C0 int primary key, C1 int, C2 int)");
    execute_statement("INSERT INTO TT VALUES (1, 1, 1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C1 AS VARCHAR(5)), CAST(C2 AS CHAR(5)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
           std::tuple{character_type(true, 5), character_type(false, 5)},
           std::forward_as_tuple(accessor::text{"1"}, accessor::text{"1    "}))), result[0]);
    }
}

TEST_F(sql_cast_test, cast_decimal) {
    execute_statement("create table TT (C0 int primary key)");
    execute_statement("INSERT INTO TT VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST('123.456' AS DECIMAL(*,*)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, std::nullopt)};
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, triple{1, 0, 123456, -3})), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST('123.456' AS DECIMAL(6,2)) FROM TT", result);
        ASSERT_EQ(1, result.size());

        auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(6, 2)};
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, triple{1, 0, 12345, -2})), result[0]);
    }
    test_stmt_err("SELECT CAST('123.456' AS DECIMAL(6,*)) FROM TT", error_code::unsupported_runtime_feature_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST('123.456' AS DECIMAL(*,2)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(std::nullopt, 2)};
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, triple{1, 0, 12345, -2})), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST('123.456' AS DECIMAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(38, 0)};
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, triple{1, 0, 123, 0})), result[0]);
    }
}

TEST_F(sql_cast_test, cast_decimal_normalize) {
    // verify the decimal values normalized when casted
    mock::basic_record::compare_decimals_as_triple_ = true;
    execute_statement("create table TT (C0 decimal(5,2) primary key)");
    execute_statement("INSERT INTO TT VALUES (1.00)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, CAST(C0 AS DECIMAL(5,2)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 2)};
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal, kind::decimal>(
                std::tuple{fm, fm},
                std::forward_as_tuple(
                    triple{1, 0, 100, -2},
                    triple{1, 0, 100, -2})
            )),
            result[0]
        );
    }
}

TEST_F(sql_cast_test, cast_bad_paraeters) {
    // compiler prevents negative precision/scale
    execute_statement("create table TT (C0 int primary key)");
    test_stmt_err("SELECT CAST('123.456' AS DECIMAL(3,-2)) FROM TT", error_code::syntax_exception);
    test_stmt_err("SELECT CAST('123.456' AS DECIMAL(-3,2)) FROM TT", error_code::syntax_exception);
}

TEST_F(sql_cast_test, cast_with_length) {
    // compiler prevents specifying length for types that doesn't support length
    execute_statement("create table TT (C0 int primary key)");
    // on new compiler, FLOAT and INT can accept length - e.g. INT(7) is TINYINT, FLOAT(53) is DOUBLE
    test_stmt_err("SELECT CAST('123.456' AS BIGINT(8)) FROM TT", error_code::syntax_exception);
    test_stmt_err("SELECT CAST('123.456' AS REAL(4)) FROM TT", error_code::syntax_exception);
    test_stmt_err("SELECT CAST('123.456' AS DOUBLE(8)) FROM TT", error_code::syntax_exception);
}

TEST_F(sql_cast_test, cast_only_with_parenthesis) {
    // compiler prevents specifying parenthesis only
    execute_statement("create table TT (C0 int primary key)");
    test_stmt_err("SELECT CAST('123.456' AS INT()) FROM TT", error_code::syntax_exception);
    test_stmt_err("SELECT CAST('123.456' AS CHAR()) FROM TT", error_code::syntax_exception);
    test_stmt_err("SELECT CAST('123.456' AS VARCHAR()) FROM TT", error_code::syntax_exception);
    test_stmt_err("SELECT CAST('123.456' AS BIGINT()) FROM TT", error_code::syntax_exception);
    test_stmt_err("SELECT CAST('123.456' AS REAL()) FROM TT", error_code::syntax_exception);
    test_stmt_err("SELECT CAST('123.456' AS DOUBLE()) FROM TT", error_code::syntax_exception);
    test_stmt_err("SELECT CAST('123.456' AS DECIMAL()) FROM TT", error_code::syntax_exception);
}

TEST_F(sql_cast_test, cast_string_with_arbitrary_length) {
    // compiler prevents specifying parenthesis only
    execute_statement("create table TT (C0 int primary key)");
    execute_statement("INSERT INTO TT (C0) VALUES (1)");
    test_stmt_err("SELECT CAST('123.456' AS CHAR(*)) FROM TT", error_code::syntax_exception);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST('123.456' AS VARCHAR(*)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"123.456"}))), result[0]);
    }
}

TEST_F(sql_cast_test, cast_without_length) {
    // CHAR is intepreted as CHAR(1)
    execute_statement("create table TT (C0 int primary key)");
    execute_statement("INSERT INTO TT VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST('123.456' AS CHAR) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(false, 1)},
           std::forward_as_tuple(accessor::text{"1"}))), result[0]);
    }
    {
        // new compiler allow VARCHAR as VARCHAR(*)
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST('123.456' AS VARCHAR) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"123.456"}))), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST('123.456' AS DECIMAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        auto fm = meta::field_type{std::make_shared<meta::decimal_field_option>(38, 0)};
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(std::tuple{fm}, triple{1, 0, 123, 0})), result[0]);
    }
}

TEST_F(sql_cast_test, cast_failure_in_where) {
    execute_statement("create table TT (C0 int primary key)");
    execute_statement("INSERT INTO TT (C0) VALUES (1)");
    test_stmt_err("SELECT C0 FROM TT WHERE C0 = CAST('bad_string' AS INT)", error_code::value_evaluation_exception);
}

TEST_F(sql_cast_test, cast_failure_vs_null) {
    // verify if evaluation failure is not ignored when comparing with null
    execute_statement("create table TT (C0 int primary key, C1 int)");
    execute_statement("INSERT INTO TT (C0) VALUES (1)");
    test_stmt_err("SELECT C0 FROM TT WHERE C1 = CAST('bad_string' AS INT)", error_code::value_evaluation_exception);
    test_stmt_err("SELECT C0 FROM TT WHERE CAST('bad_string' AS INT) = C1", error_code::value_evaluation_exception);
}

TEST_F(sql_cast_test, cast_float8_nan) {
    execute_statement("create table TT (C0 double primary key)");
    execute_statement("INSERT INTO TT (C0) VALUES (CAST('NaN' AS DOUBLE))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto meta = rec.record_meta();
        auto v = rec.get_value<double>(0);
        EXPECT_TRUE(std::isnan(v));
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(*)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"NaN"}))), result[0]);
    }
}
TEST_F(sql_cast_test, cast_float4_nan) {
    execute_statement("create table TT (C0 real primary key)");
    execute_statement("INSERT INTO TT (C0) VALUES (CAST('NaN' AS REAL))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto meta = rec.record_meta();
        auto v = rec.get_value<float>(0);
        EXPECT_TRUE(std::isnan(v));
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(*)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"NaN"}))), result[0]);
    }
}

TEST_F(sql_cast_test, cast_float8_inf) {
    execute_statement("create table TT (C0 double primary key)");
    execute_statement("INSERT INTO TT (C0) VALUES (CAST('Infinity' AS DOUBLE))");
    execute_statement("INSERT INTO TT (C0) VALUES (CAST('-Infinity' AS DOUBLE))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT WHERE C0 < 0", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto meta = rec.record_meta();
        auto v = rec.get_value<double>(0);
        EXPECT_TRUE(std::isinf(v));
        EXPECT_TRUE(std::signbit(v));
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT WHERE C0 > 0", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto meta = rec.record_meta();
        auto v = rec.get_value<double>(0);
        EXPECT_TRUE(std::isinf(v));
        EXPECT_FALSE(std::signbit(v));
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(*)) FROM TT ORDER BY C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"-Infinity"}))), result[0]);
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"Infinity"}))), result[1]);
    }
}
TEST_F(sql_cast_test, cast_float4_inf) {
    execute_statement("create table TT (C0 real primary key)");
    execute_statement("INSERT INTO TT (C0) VALUES (CAST('Infinity' AS REAL))");
    execute_statement("INSERT INTO TT (C0) VALUES (CAST('-Infinity' AS REAL))");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT WHERE C0 < 0", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto meta = rec.record_meta();
        auto v = rec.get_value<float>(0);
        EXPECT_TRUE(std::isinf(v));
        EXPECT_TRUE(std::signbit(v));
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT WHERE C0 > 0", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        auto meta = rec.record_meta();
        auto v = rec.get_value<float>(0);
        EXPECT_TRUE(std::isinf(v));
        EXPECT_FALSE(std::signbit(v));
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(C0 AS VARCHAR(*)) FROM TT ORDER BY C0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"-Infinity"}))), result[0]);
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"Infinity"}))), result[1]);
    }
}

TEST_F(sql_cast_test, float_inf_to_decimal) {
    // inf/-inf converted to triple max/min
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(CAST('Infinity' AS DOUBLE) AS DECIMAL(*,*)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
           std::tuple{decimal_type(std::nullopt, std::nullopt)},
           executor::expr::details::triple_max)), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(CAST('-Infinity' AS DOUBLE) AS DECIMAL(*,*)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
           std::tuple{decimal_type(std::nullopt, std::nullopt)},
           executor::expr::details::triple_min)), result[0]);
    }
}

TEST_F(sql_cast_test, triple_max_min_string_repr) {
    // check string representation of triple max/min
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(CAST(CAST('Infinity' AS DOUBLE) AS DECIMAL(*,*)) AS VARCHAR(*)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"9.9999999999999999999999999999999999999E+24576"}))), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(CAST(CAST('-Infinity' AS DOUBLE) AS DECIMAL(*,*)) AS VARCHAR(*)) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::character>(
           std::tuple{character_type(true, std::nullopt)},
           std::forward_as_tuple(accessor::text{"-9.9999999999999999999999999999999999999E+24576"}))), result[0]);
    }
}

TEST_F(sql_cast_test, float_to_string_round_trip) {
    execute_statement("create table TT (C0 INT primary key)");
    execute_statement("INSERT INTO TT VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(CAST(CAST('0.1' AS DOUBLE) AS VARCHAR(*)) AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float8>({0.1}, {false})), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(CAST(CAST('-0.1' AS REAL) AS VARCHAR(*)) AS REAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float4>({-0.1}, {false})), result[0]);
    }
    {
        // approx result is ok
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(CAST(CAST('1.79769e+308' AS DOUBLE) AS VARCHAR(*)) AS DOUBLE) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float8>({1.79769e+308}, {false})), result[0]);
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT CAST(CAST(CAST('3.40282e+38' AS REAL) AS VARCHAR(*)) AS REAL) FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::float4>({3.40282e+38}, {false})), result[0]);
    }
}

TEST_F(sql_cast_test, format_error_with_too_large_number) {
    execute_statement("create table TT (C0 int primary key)");
    execute_statement("INSERT INTO TT VALUES (1)");
    test_stmt_err("SELECT CAST('1E+30000' AS DECIMAL(*,*)) FROM TT", error_code::value_evaluation_exception);
    test_stmt_err("SELECT CAST('1E-30000' AS DECIMAL(*,*)) FROM TT", error_code::value_evaluation_exception);
}

TEST_F(sql_cast_test, unsupported_small_integers) {
    execute_statement("create table TT (C0 int primary key)");
    execute_statement("INSERT INTO TT VALUES (1)");
    test_stmt_err("SELECT CAST('true' AS BOOLEAN) FROM TT", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("SELECT CAST('1' AS TINYINT) FROM TT", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("SELECT CAST('1' AS SMALLINT) FROM TT", error_code::unsupported_runtime_feature_exception);
}

}  // namespace jogasaki::testing
