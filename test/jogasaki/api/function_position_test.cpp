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

class function_position_test : public ::testing::Test, public api_test_base {

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
    std::string substr;
    int result;
};

using namespace std::string_view_literals;

TEST_F(function_position_test, varchar) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(40))");
    execute_statement("insert into t values ('aéあ𠮷bいうa')");
    std::vector<TestCase> test_cases = {{"a", 1}, {"é", 2}, {"あ", 3}, {"𠮷", 4}, {"b", 5},
        {"い", 6}, {"う", 7}, {"aé", 1}, {"éあ", 2}, {"あ𠮷", 3}, {"𠮷b", 4}, {"bい", 5},
        {"いう", 6}, {"うa", 7}, {"aéあ", 1}, {"éあ𠮷", 2}, {"あ𠮷b", 3}, {"𠮷bい", 4},
        {"bいう", 5}, {"いうa", 6}, {"aéあ𠮷", 1}, {"éあ𠮷b", 2}, {"あ𠮷bい", 3}, {"𠮷bいう", 4},
        {"bいうa", 5}, {"aéあ𠮷b", 1}, {"éあ𠮷bい", 2}, {"あ𠮷bいう", 3}, {"𠮷bいうa", 4},
        {"aéあ𠮷bい", 1}, {"éあ𠮷bいう", 2}, {"あ𠮷bいうa", 3}, {"aéあ𠮷bいう", 1},
        {"éあ𠮷bいうa", 2}, {"aéあ𠮷bいうa", 1}, {"aéあ𠮷bいうab", 0}, {"c", 0}, {"ä", 0},
        {"ヤ", 0}, {"🍺", 0}, {"cä", 0}, {"äヤ", 0}, {"ヤ🍺", 0}, {"cäヤ", 0}, {"äヤ🍺", 0},
        {"cäヤ🍺", 0}, {"ab", 0}, {"", 1}, {" ", 0}};
    for (const auto& test : test_cases) {
        std::vector<mock::basic_record> result{};
        std::string query =
            std::string("SELECT position('") + test.substr + std::string("' IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ(create_nullable_record<kind::int8>(test.result), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_position_test, char) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 char(40))");
    execute_statement("insert into t values ('aéあ𠮷bいうa')");
    std::vector<TestCase> test_cases = {{"a", 1}, {"é", 2}, {"あ", 3}, {"𠮷", 4}, {"b", 5},
        {"い", 6}, {"う", 7}, {"aé", 1}, {"éあ", 2}, {"あ𠮷", 3}, {"𠮷b", 4}, {"bい", 5},
        {"いう", 6}, {"うa", 7}, {"aéあ", 1}, {"éあ𠮷", 2}, {"あ𠮷b", 3}, {"𠮷bい", 4},
        {"bいう", 5}, {"いうa", 6}, {"aéあ𠮷", 1}, {"éあ𠮷b", 2}, {"あ𠮷bい", 3}, {"𠮷bいう", 4},
        {"bいうa", 5}, {"aéあ𠮷b", 1}, {"éあ𠮷bい", 2}, {"あ𠮷bいう", 3}, {"𠮷bいうa", 4},
        {"aéあ𠮷bい", 1}, {"éあ𠮷bいう", 2}, {"あ𠮷bいうa", 3}, {"aéあ𠮷bいう", 1},
        {"éあ𠮷bいうa", 2}, {"aéあ𠮷bいうa", 1}, {"aéあ𠮷bいうab", 0}, {"c", 0}, {"ä", 0},
        {"ヤ", 0}, {"🍺", 0}, {"cä", 0}, {"äヤ", 0}, {"ヤ🍺", 0}, {"cäヤ", 0}, {"äヤ🍺", 0},
        {"cäヤ🍺", 0}, {"ab", 0}, {"", 1}, {" ", 9}};
    for (const auto& test : test_cases) {
        std::vector<mock::basic_record> result{};
        std::string query =
            std::string("SELECT position('") + test.substr + std::string("' IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ(create_nullable_record<kind::int8>(test.result), result[0])
            << "Failed query: " << query;
    }
}
TEST_F(function_position_test, empty) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(40))");
    execute_statement("insert into t values ('')");
    std::vector<TestCase> test_cases = {{"a", 0}, {"", 1}};
    for (const auto& test : test_cases) {
        std::vector<mock::basic_record> result{};
        std::string query =
            std::string("SELECT position('") + test.substr + std::string("' IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ(create_nullable_record<kind::int8>(test.result), result[0])
            << "Failed query: " << query;
    }
}

TEST_F(function_position_test, string_null) {
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values (NULL)");
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(NULL IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position('a' IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
}

TEST_F(function_position_test, substring_null) {
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values ('a')");
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(NULL IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
}

TEST_F(function_position_test, invalidutf8_1byte_sub) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\x80");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position('a' IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(c0 IN 'a' ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ(create_nullable_record<kind::int8>(0), result[0]) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(c0 IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
}

TEST_F(function_position_test, invalid_utf8_2byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xC0\x80");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position('a' IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(c0 IN 'a' ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ(create_nullable_record<kind::int8>(0), result[0]) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(c0 IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
}

TEST_F(function_position_test, invalid_utf8_3byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xE2\x28\xA1");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position('a' IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(c0 IN 'a' ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ(create_nullable_record<kind::int8>(0), result[0]) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(c0 IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
}

TEST_F(function_position_test, invalid_utf8_4byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xF4\x27\x80\x80");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position('a' IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(c0 IN 'a' ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_EQ(create_nullable_record<kind::int8>(0), result[0]) << "Failed query: " << query;
    }
    {
        std::vector<mock::basic_record> result{};
        std::string query = std::string("SELECT position(c0 IN c0 ) FROM t");
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
    }
}

} // namespace jogasaki::testing
