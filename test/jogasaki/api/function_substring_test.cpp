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

class function_substring_test :
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

TEST_F(function_substring_test, varbinary) {
    execute_statement("create table t (c0 varbinary(20))");
    execute_statement("insert into t values ('01c2e0f0bf')");
    std::vector<TestCase> test_cases = {
        {-1, std::nullopt, std::nullopt},
        {0, std::nullopt, std::nullopt}, 
        {1, std::nullopt, "\x01\xC2\xE0\xF0\xBF"}, 
        {2, std::nullopt, "\xC2\xE0\xF0\xBF"},
        {3, std::nullopt, "\xE0\xF0\xBF"},
        {4, std::nullopt, "\xF0\xBF"}, 
        {5, std::nullopt, "\xBF"},
        {6, std::nullopt, std::nullopt}, 
        {-1, -1, std::nullopt}, 
        {-1, 0, std::nullopt},
        {-1, 1, std::nullopt}, 
        {0, -1, std::nullopt}, 
        {0, 0, std::nullopt}, 
        {0, 1, std::nullopt},
        {1, -5, std::nullopt},
        {1, -4, std::nullopt},
        {1, -3, std::nullopt},
        {1, -2, std::nullopt},
        {1, -1, std::nullopt},
        {1, 0, ""}, 
        {1, 1, "\x01"}, 
        {1, 2, "\x01\xC2"}, 
        {1, 3, "\x01\xC2\xE0"}, 
        {1, 4, "\x01\xC2\xE0\xF0"},
        {1, 5, "\x01\xC2\xE0\xF0\xBF"}, 
        {1, 6, "\x01\xC2\xE0\xF0\xBF"},
        {2, -4, std::nullopt},
        {2, -3, std::nullopt},
        {2, -2, std::nullopt},
        {2, -1, std::nullopt},
        {2, 0, ""},
        {2, 1, "\xC2"},
        {2, 2, "\xC2\xE0"},
        {2, 3, "\xC2\xE0\xF0"},
        {2, 4, "\xC2\xE0\xF0\xBF"},
        {2, 5, "\xC2\xE0\xF0\xBF"},
        {3, -3, std::nullopt},
        {3, -2, std::nullopt},
        {3, -1, std::nullopt},
        {3, 0, ""},
        {3, 1, "\xE0"},
        {3, 2, "\xE0\xF0"},
        {3, 3, "\xE0\xF0\xBF"},
        {3, 4, "\xE0\xF0\xBF"},
        {4, -2, std::nullopt},
        {4, -1, std::nullopt},
        {4, 0, ""},
        {4, 1, "\xF0"},
        {4, 2, "\xF0\xBF"},
        {4, 3, "\xF0\xBF"},
        {5, -1, std::nullopt},
        {5, 0, ""},
        {5, 1, "\xBF"},
        {5, 2, "\xBF"},
        {6, -1, std::nullopt},
        {6, 0, std::nullopt},
        {6, 1, std::nullopt}
    };
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT substring(c0 FROM ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " FOR " + std::to_string(test.for_value.value());
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
TEST_F(function_substring_test, binary) {
    execute_statement("create table t (c0 binary(20))");
    execute_statement("insert into t values ('01c2e0f0bf')");
    std::vector<TestCase> test_cases = {
        {-1, std::nullopt, std::nullopt},
        {0, std::nullopt, std::nullopt}, 
        {1, std::nullopt, std::string("\x01\xC2\xE0\xF0\xBF\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",20)},
        {2, std::nullopt, std::string("\xC2\xE0\xF0\xBF\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",19)},
        {3, std::nullopt, std::string("\xE0\xF0\xBF\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",18)},
        {4, std::nullopt, std::string("\xF0\xBF\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",17)},
        {5, std::nullopt, std::string("\xBF\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",16)},
        {6, std::nullopt, std::string("\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00",15)},
        {-1, -1, std::nullopt}, 
        {-1, 0, std::nullopt},
        {-1, 1, std::nullopt}, 
        {0, -1, std::nullopt}, 
        {0, 0, std::nullopt}, 
        {0, 1, std::nullopt},
        {1, -5, std::nullopt},
        {1, -4, std::nullopt},
        {1, -3, std::nullopt},
        {1, -2, std::nullopt},
        {1, -1, std::nullopt},
        {1, 0, ""}, 
        {1, 1, "\x01"},
        {1, 2, "\x01\xC2"},
        {1, 3, "\x01\xC2\xE0"},
        {1, 4, "\x01\xC2\xE0\xF0"},
        {1, 5, "\x01\xC2\xE0\xF0\xBF"},
        {1, 6, std::string("\x01\xC2\xE0\xF0\xBF\x00",6)},
        {2, -4, std::nullopt},
        {2, -3, std::nullopt},
        {2, -2, std::nullopt},
        {2, -1, std::nullopt},
        {2, 0, ""} ,
        {2, 1, "\xC2"},
        {2, 2, "\xC2\xE0"},
        {2, 3, "\xC2\xE0\xF0"},
        {2, 4, "\xC2\xE0\xF0\xBF"},
        {2, 5,  std::string("\xC2\xE0\xF0\xBF\x00",5)},
        {3, -3, std::nullopt},
        {3, -2, std::nullopt},
        {3, -1, std::nullopt},
        {3, 0, ""},
        {3, 1, "\xE0"},
        {3, 2, "\xE0\xF0"},
        {3, 3, "\xE0\xF0\xBF"},
        {3, 4, std::string("\xE0\xF0\xBF\x00",4)},
        {4, -2, std::nullopt},
        {4, -1, std::nullopt},
        {4, 0, ""},
        {4, 1, "\xF0"},
        {4, 2, "\xF0\xBF"},
        {4, 3, std::string("\xF0\xBF\x00",3)},
        {5, -1, std::nullopt},
        {5, 0, ""},
        {5, 1, "\xBF"},
        {5, 2, std::string("\xBF\x00",2)},
        {6, -1, std::nullopt},
        {6, 0, ""},
        {6, 1, std::string("\x00",1)}
    };
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT substring(c0 FROM ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " FOR " + std::to_string(test.for_value.value());
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
TEST_F(function_substring_test, varchar) {
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values ('aéあ𠮷b')");
    std::vector<TestCase> test_cases = {
        {-1, std::nullopt, std::nullopt},
        {0, std::nullopt, std::nullopt}, 
        {1, std::nullopt, "aéあ𠮷b"}, 
        {2, std::nullopt, "éあ𠮷b"},
        {3, std::nullopt, "あ𠮷b"}, 
        {4, std::nullopt, "𠮷b"}, 
        {5, std::nullopt, "b"},
        {6, std::nullopt, std::nullopt}, 
        {-1, -1, std::nullopt}, 
        {-1, 0, std::nullopt},
        {-1, 1, std::nullopt}, 
        {0, -1, std::nullopt}, 
        {0, 0, std::nullopt}, 
        {0, 1, std::nullopt},
        {1, -5, std::nullopt},
        {1, -4, std::nullopt},
        {1, -3, std::nullopt},
        {1, -2, std::nullopt},
        {1, -1, std::nullopt},
        {1, 0, ""}, 
        {1, 1, "a"}, 
        {1, 2, "aé"}, 
        {1, 3, "aéあ"}, 
        {1, 4, "aéあ𠮷"},
        {1, 5, "aéあ𠮷b"}, 
        {1, 6, "aéあ𠮷b"},
        {2, -4, std::nullopt},
        {2, -3, std::nullopt},
        {2, -2, std::nullopt},
        {2, -1, std::nullopt},
        {2, 0, ""},
        {2, 1, "é"},
        {2, 2, "éあ"},
        {2, 3, "éあ𠮷"},
        {2, 4, "éあ𠮷b"},
        {2, 5, "éあ𠮷b"},
        {3, -3, std::nullopt},
        {3, -2, std::nullopt},
        {3, -1, std::nullopt},
        {3, 0, ""},
        {3, 1, "あ"},
        {3, 2, "あ𠮷"},
        {3, 3, "あ𠮷b"},
        {3, 4, "あ𠮷b"},
        {4, -2, std::nullopt},
        {4, -1, std::nullopt},
        {4, 0, ""},
        {4, 1, "𠮷"},
        {4, 2, "𠮷b"},
        {4, 3, "𠮷b"},
        {5, -1, std::nullopt},
        {5, 0, ""},
        {5, 1, "b"},
        {5, 2, "b"},
        {6, -1, std::nullopt},
        {6, 0, std::nullopt},
        {6, 1, std::nullopt}
    };
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT substring(c0 FROM ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " FOR " + std::to_string(test.for_value.value());
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

TEST_F(function_substring_test, char) {
    execute_statement("create table t (c0 char(20))");
    execute_statement("insert into t values ('aéあ𠮷b')");
    // aéあ𠮷b is 1+2+3+4+1 =11byte
    std::vector<TestCase> test_cases = {
        {-1, std::nullopt, std::nullopt},
        {0, std::nullopt, std::nullopt}, 
        {1, std::nullopt, "aéあ𠮷b         "}, // char[21] because +1 nullterminate
        {2, std::nullopt, "éあ𠮷b         "},  // char[20] because char[21] -a(1byte)
        {3, std::nullopt, "あ𠮷b         "},   // char[18] becase char[20] -é(2byte)
        {4, std::nullopt, "𠮷b         "},     // char[15] becase char[18] -あ(3byte)
        {5, std::nullopt, "b         "},       // char[11] becase char[15] -𠮷(4byte)
        {6, std::nullopt, "         "},        // char[10] becase char[11] -b(1byte)
        {-1, -1, std::nullopt}, 
        {-1, 0, std::nullopt},
        {-1, 1, std::nullopt}, 
        {0, -1, std::nullopt}, 
        {0, 0, std::nullopt}, 
        {0, 1, std::nullopt},
        {1, -5, std::nullopt},
        {1, -4, std::nullopt},
        {1, -3, std::nullopt},
        {1, -2, std::nullopt},
        {1, -1, std::nullopt},
        {1, 0, ""}, 
        {1, 1, "a"}, 
        {1, 2, "aé"}, 
        {1, 3, "aéあ"}, 
        {1, 4, "aéあ𠮷"},
        {1, 5, "aéあ𠮷b"}, 
        {1, 6, "aéあ𠮷b "},
        {2, -4, std::nullopt},
        {2, -3, std::nullopt},
        {2, -2, std::nullopt},
        {2, -1, std::nullopt},
        {2, 0, ""},
        {2, 1, "é"},
        {2, 2, "éあ"},
        {2, 3, "éあ𠮷"},
        {2, 4, "éあ𠮷b"},
        {2, 5, "éあ𠮷b "},
        {3, -3, std::nullopt},
        {3, -2, std::nullopt},
        {3, -1, std::nullopt},
        {3, 0, ""},
        {3, 1, "あ"},
        {3, 2, "あ𠮷"},
        {3, 3, "あ𠮷b"},
        {3, 4, "あ𠮷b "},
        {4, -2, std::nullopt},
        {4, -1, std::nullopt},
        {4, 0, ""},
        {4, 1, "𠮷"},
        {4, 2, "𠮷b"},
        {4, 3, "𠮷b "},
        {5, -1, std::nullopt},
        {5, 0, ""},
        {5, 1, "b"},
        {5, 2, "b "},
        {6, -1, std::nullopt},
        {6, 0, ""},
        {6, 1, " "}
    };
    for (const auto& test :test_cases) {
        std::string query =
            std::string("SELECT substring(c0 FROM ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " FOR " + std::to_string(test.for_value.value());
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
TEST_F(function_substring_test, null) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(20))");
    execute_statement("insert into t values (null)");
    std::vector<TestCase> test_cases = {
        {-1, std::nullopt, std::nullopt},
        {0, std::nullopt, std::nullopt},
        {1, std::nullopt, std::nullopt},
        {2, std::nullopt, std::nullopt},
        {3, std::nullopt, std::nullopt},
        {4, std::nullopt, std::nullopt},
        {5, std::nullopt, std::nullopt},
        {6, std::nullopt, std::nullopt},
        {-1, -1, std::nullopt}, 
        {-1, 0, std::nullopt},
        {-1, 1, std::nullopt}, 
        {0, -1, std::nullopt}, 
        {0, 0, std::nullopt}, 
        {0, 1, std::nullopt},
        {1, -5, std::nullopt},
        {1, -4, std::nullopt},
        {1, -3, std::nullopt},
        {1, -2, std::nullopt},
        {1, -1, std::nullopt},
        {1, 0, std::nullopt},
        {1, 1, std::nullopt},
        {1, 2, std::nullopt},
        {1, 3, std::nullopt},
        {1, 4, std::nullopt},
        {1, 5, std::nullopt},
        {1, 6, std::nullopt},
        {2, -4, std::nullopt},
        {2, -3, std::nullopt},
        {2, -2, std::nullopt},
        {2, -1, std::nullopt},
        {2, 0, std::nullopt},
        {2, 1, std::nullopt},
        {2, 2, std::nullopt},
        {2, 3, std::nullopt},
        {2, 4, std::nullopt},
        {2, 5, std::nullopt},
        {3, -3, std::nullopt},
        {3, -2, std::nullopt},
        {3, -1, std::nullopt},
        {3, 0, std::nullopt},
        {3, 1, std::nullopt},
        {3, 2, std::nullopt},
        {3, 3, std::nullopt},
        {3, 4, std::nullopt},
        {4, -2, std::nullopt},
        {4, -1, std::nullopt},
        {4, 0, std::nullopt},
        {4, 1, std::nullopt},
        {4, 2, std::nullopt},
        {4, 3, std::nullopt},
        {5, -1, std::nullopt},
        {5, 0, std::nullopt},
        {5, 1, std::nullopt},
        {5, 2, std::nullopt},
        {6, -1, std::nullopt},
        {6, 0, std::nullopt},
        {6, 1, std::nullopt},
    };
    for (const auto& test :test_cases) {
        std::string query =
            std::string("SELECT substring(c0 FROM ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " FOR " + std::to_string(test.for_value.value());
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

TEST_F(function_substring_test, invalidutf8_1byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\x80");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);

    std::vector<TestCase> test_cases = {{1, std::nullopt, std::nullopt}};
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT substring(c0 FROM ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " FOR " + std::to_string(test.for_value.value());
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

TEST_F(function_substring_test, invalid_utf8_2byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xC0\x80");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);

    std::vector<TestCase> test_cases = {{1, std::nullopt, std::nullopt}};
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT substring(c0 FROM ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " FOR " + std::to_string(test.for_value.value());
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
TEST_F(function_substring_test, invalid_utf8_3byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xE2\x28\xA1");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);

    std::vector<TestCase> test_cases = {{1, std::nullopt, std::nullopt}};
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT substring(c0 FROM ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " FOR " + std::to_string(test.for_value.value());
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

TEST_F(function_substring_test, invalid_utf8_4byte) {
    execute_statement("create table t (c0 varchar(100))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character}};
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "\xF4\x27\x80\x80");
    execute_statement("INSERT INTO t (c0) VALUES (:p0)", variables, *ps);

    std::vector<TestCase> test_cases = {{1, std::nullopt, std::nullopt}};
    for (const auto& test : test_cases) {
        std::string query =
            std::string("SELECT substring(c0 FROM ") + std::to_string(test.from_value);
        std::vector<mock::basic_record> result{};
        if (test.for_value.has_value()) {
            query += " FOR " + std::to_string(test.for_value.value());
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

}  // namespace jogasaki::testing
