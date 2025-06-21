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

class function_decode_test : public ::testing::Test, public api_test_base {

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
    int from_value;
    std::optional<int> for_value;
    std::optional<std::string> expected;
};

using namespace std::string_view_literals;

TEST_F(function_decode_test, basic) {
    std::string input = std::string("AQ==");
    std::string res   = std::string("\x01");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    std::string query = std::string("SELECT decode(c0,'base64') FROM t ");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_binary(res);
    EXPECT_EQ(create_nullable_record<kind::octet>(expected_binary), result[0])
        << "Failed query: " << query;
}
TEST_F(function_decode_test, two) {
    std::string input = std::string("QUI=");
    std::string res   = std::string("\x41\x42");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    std::string query = std::string("SELECT decode(c0,'Base64') FROM t ");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_text(res);
    EXPECT_EQ(create_nullable_record<kind::octet>(expected_text), result[0])
        << "Failed query: " << query;
}
TEST_F(function_decode_test, three) {
    std::string input = std::string("QUJD");
    std::string res   = std::string("\x41\x42\x43");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    std::string query = std::string("SELECT decode(c0,'bAse64') FROM t ");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_text(res);
    EXPECT_EQ(create_nullable_record<kind::octet>(expected_text), result[0])
        << "Failed query: " << query;
}
TEST_F(function_decode_test, four) {
    std::string input = std::string("QUJDRA==");
    std::string res   = std::string("\x41\x42\x43\x44");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    std::string query = std::string("SELECT decode(c0,'baSe64') FROM t ");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_text(res);
    EXPECT_EQ(create_nullable_record<kind::octet>(expected_text), result[0])
        << "Failed query: " << query;
}
TEST_F(function_decode_test, four_over) {
    std::string input = std::string("3q2+7w==");
    std::string res   = std::string("\xDE\xAD\xBE\xEF");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    std::string query = std::string("SELECT decode(c0,'basE64') FROM t ");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_text(res);
    EXPECT_EQ(create_nullable_record<kind::octet>(expected_text), result[0])
        << "Failed query: " << query;
}
TEST_F(function_decode_test, empty) {
    std::string input = std::string("");
    std::string res   = std::string("");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    std::string query = std::string("SELECT decode(c0,'basE64') FROM t ");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_text(res);
    EXPECT_EQ(create_nullable_record<kind::octet>(expected_text), result[0])
        << "Failed query: " << query;
}
TEST_F(function_decode_test, notbase64) {
    std::string input = std::string("3q2+7w==");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    {
        test_stmt_err(
            "SELECT decode(c0,'base11') FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_decode_test, null) {
    std::string input = std::string("null");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    std::string query  = std::string("SELECT decode(c0,'BASE64') FROM t ");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(0, result.size()) << "Query failed: " << query;
}

TEST_F(function_decode_test, base64varchar) {
    std::string input = std::string("UEkoSSIzIlRomQ==");
    std::string res   = std::string("\x50\x49\x28\x49\x22\x33\x22\x54\x68\x99");
    execute_statement("create table t (c0 varchar(20) ,c1 varchar(20))");
    std::string insert = "insert into t values ('" + input + "' ,'base64' )";
    execute_statement(insert);
    std::string query = std::string("SELECT decode(c0,'BAse64') FROM t ");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_text(res);
    EXPECT_EQ(create_nullable_record<kind::octet>(expected_text), result[0])
        << "Failed query: " << query;
}
TEST_F(function_decode_test, notchar) {
    std::string input = std::string("0001");
    execute_statement("create table t (c0 varbinary(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    { test_stmt_err("SELECT decode(c0,'base64') FROM t", error_code::symbol_analyze_exception); }
}

TEST_F(function_decode_test, all) {
    std::string input =
        std::string("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789+/");
    std::string res =
        std::string("\x69\xB7\x1D\x79\xF8\x21\x8A\x39\x25\x9A\x7A\x29\xAA\xBB\x2D\xBA\xFC\x31\xCB"
                    "\x30\x01\x08\x31\x05\x18\x72\x09\x28\xB3\x0D\x38\xF4\x11\x49\x35\x15\x59\x76"
                    "\x19\xD3\x5D\xB7\xE3\x9E\xBB\xF3\xDF\xBF");
    execute_statement("create table t (c0 varchar(80))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    std::string query = std::string("SELECT decode(c0,'Base64') FROM t ");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_text(res);
    EXPECT_EQ(create_nullable_record<kind::octet>(expected_text), result[0])
        << "Failed query: " << query;
}
TEST_F(function_decode_test, invalidone) {
    std::string input = std::string("a");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    {
        test_stmt_err(
            "SELECT decode(c0,'base64') FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_decode_test, invalidtwo) {
    std::string input = std::string("ab");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    {
        test_stmt_err(
            "SELECT decode(c0,'base64') FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_decode_test, invalidthree) {
    std::string input = std::string("abc");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    {
        test_stmt_err(
            "SELECT decode(c0,'base64') FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_decode_test, invalidonepadding) {
    std::string input = std::string("a=");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    {
        test_stmt_err(
            "SELECT decode(c0,'base64') FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_decode_test, invalidonetwopadding) {
    std::string input = std::string("a==");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    {
        test_stmt_err(
            "SELECT decode(c0,'base64') FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_decode_test, invalidonethrepadding) {
    std::string input = std::string("a===");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    {
        test_stmt_err(
            "SELECT decode(c0,'base64') FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
TEST_F(function_decode_test, allpadding) {
    std::string input = std::string("====");
    execute_statement("create table t (c0 varchar(20))");
    std::string insert = "insert into t values ('" + input + "')";
    execute_statement(insert);
    {
        test_stmt_err(
            "SELECT decode(c0,'base64') FROM t", error_code::unsupported_runtime_feature_exception);
    }
}
} // namespace jogasaki::testing
