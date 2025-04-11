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

class function_substr_test :
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
    int from_value;
    std::optional<int> for_value;
    std::optional<std::string> expected;
};

using namespace std::string_view_literals;

TEST_F(function_substr_test, varbinary) {
    execute_statement("create table t (c0 varbinary(20))");
    execute_statement("insert into t values ('01c2e0f0bf')");
    std::vector<TestCase> test_cases = {
                {6, 1, std::nullopt}
    };
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT substr(c0 , ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " , " + std::to_string(test.for_value.value());
        }
        query += ") FROM t";
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        if (test.expected) {
            accessor::binary expected_text(*test.expected);
            EXPECT_EQ(create_nullable_record<kind::octet>(accessor::binary{expected_text}), result[0])
                << "Failed query: " << query;
        } else {
            EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
        }
    }
}
TEST_F(function_substr_test, binary) {
    execute_statement("create table t (c0 binary(20))");
    execute_statement("insert into t values ('01c2e0f0bf')");
    std::vector<TestCase> test_cases = {
        {6, 1, std::string("\x00",1)}
    };
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT SUBSTR(c0 , ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " , " + std::to_string(test.for_value.value());
        }
        query += ") FROM t";
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        if (test.expected) {
            accessor::binary expected_text(*test.expected);
            EXPECT_EQ(create_nullable_record<kind::octet>(accessor::binary{expected_text}), result[0])
                << "Failed query: " << query;
        } else {
            EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
        }
    }
}
TEST_F(function_substr_test, varchar) {
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values ('aéあ𠮷b')");
    std::vector<TestCase> test_cases = {
        {6, 1, std::nullopt}
    };
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT Substr(c0 , ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " , " + std::to_string(test.for_value.value());
        }
        query += ") FROM t";
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        if (test.expected) {
            accessor::text expected_text(*test.expected);
            EXPECT_EQ(create_nullable_record<kind::character>(expected_text), result[0])
                << "Failed query: " << query;
        } else {
            EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
        }
    }
}

TEST_F(function_substr_test, char) {
    execute_statement("create table t (c0 char(20))");
    execute_statement("insert into t values ('aéあ𠮷b')");
    std::vector<TestCase> test_cases = {
        {6, 1, " "}
    };
    for (const auto& test :test_cases) {
        std::string query =
            std::string("SELECT SUbstr(c0 , ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " , " + std::to_string(test.for_value.value());
        }
        query += ") FROM t";
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        if (test.expected) {
            accessor::text expected_text(*test.expected);
            EXPECT_EQ(create_nullable_record<kind::character>(expected_text), result[0])
                << "Failed query: " << query;
        } else {
            EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
        }
    }
}
TEST_F(function_substr_test, null) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values (null)");
    std::vector<TestCase> test_cases = {
        {6, 1, std::nullopt},
    };
    for (const auto& test :test_cases) {
        std::string query =
            std::string("SELECT SUBstr(c0 , ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " , " + std::to_string(test.for_value.value());
        }
        query += ") FROM t";
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        if (test.expected) {
            accessor::text expected_text(*test.expected);
            EXPECT_EQ(create_nullable_record<kind::character>(expected_text), result[0])
                << "Failed query: " << query;
        } else {
            EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
        }
    } 
}

TEST_F(function_substr_test, invalidutf8_1byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\x80");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);

    std::vector<TestCase> test_cases = {{1, std::nullopt, std::nullopt}};
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT SUBStr(c0 , ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " , " + std::to_string(test.for_value.value());
        }
        query += ") FROM t";
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        if (test.expected) {
            accessor::text expected_text(*test.expected);
            EXPECT_EQ(create_nullable_record<kind::character>(expected_text), result[0])
                << "Failed query: " << query;
        } else {
            EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
        }
    }
}

TEST_F(function_substr_test, invalid_utf8_2byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xC0\x80");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);

    std::vector<TestCase> test_cases = {{1, std::nullopt, std::nullopt}};
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT SUBSTr(c0 , ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " , " + std::to_string(test.for_value.value());
        }
        query += ") FROM t";
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        if (test.expected) {
            accessor::text expected_text(*test.expected);
            EXPECT_EQ(create_nullable_record<kind::character>(expected_text), result[0])
                << "Failed query: " << query;
        } else {
            EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
        }
    }
}
TEST_F(function_substr_test, invalid_utf8_3byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xE2\x28\xA1");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);

    std::vector<TestCase> test_cases = {{1, std::nullopt, std::nullopt}};
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT substR(c0 , ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " , " + std::to_string(test.for_value.value());
        }
        query += ") FROM t";
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        if (test.expected) {
            accessor::text expected_text(*test.expected);
            EXPECT_EQ(create_nullable_record<kind::character>(expected_text), result[0])
                << "Failed query: " << query;
        } else {
            EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
        }
    }
}

TEST_F(function_substr_test, invalid_utf8_4byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xF4\x27\x80\x80");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);

    std::vector<TestCase> test_cases = {{1, std::nullopt, std::nullopt}};
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT subsTR(c0 , ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " , " + std::to_string(test.for_value.value());
        }
        query += ") FROM t";
        execute_query(query, result);
        ASSERT_EQ(1, result.size()) << "Query failed: " << query;
        if (test.expected) {
            accessor::text expected_text(*test.expected);
            EXPECT_EQ(create_nullable_record<kind::character>(expected_text), result[0])
                << "Failed query: " << query;
        } else {
            EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
        }
    }
}

TEST_F(function_substr_test, 3rd_null) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values ('abcde')");
    std::string query = std::string("SELECT subSTR(c0 , 1 , NULL ) FROM t");
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
}

TEST_F(function_substr_test, 2nd_3rd_null) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values ('abcde')");
    std::string query = std::string("SELECT suBSTR(c0 , NULL , NULL ) FROM t");
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
}

TEST_F(function_substr_test, 2nd_null) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values ('abcde')");
    std::string query = std::string("SELECT sUBSTR(c0 , NULL ) FROM t");
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
}


TEST_F(function_substr_test, 2nd_null_3rd_number) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values ('abcde')");
    std::string query = std::string("SELECT suBsTr(c0 , NULL , 2 ) FROM t");
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    EXPECT_TRUE(result[0].is_null(0)) << "Failed query: " << query;
}

}  // namespace jogasaki::testing
