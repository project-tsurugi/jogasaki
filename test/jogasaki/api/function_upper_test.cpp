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

class function_upper_test :
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

TEST_F(function_upper_test, varchar) {
    execute_statement("create table t (c0 varchar(70))");
    execute_statement("insert into t values ('éあ𠮷abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')");

    std::string query = std::string("SELECT upper(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::text expected_text("éあ𠮷ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ");
    EXPECT_EQ(create_nullable_record<kind::character>(expected_text), result[0])
        << "Failed query: " << query;
}
TEST_F(function_upper_test, char) {
    execute_statement("create table t (c0 char(70))");
    execute_statement("insert into t values ('éあ𠮷abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')");

    std::string query = std::string("SELECT upper(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::text expected_text("éあ𠮷ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJKLMNOPQRSTUVWXYZ         ");
    EXPECT_EQ(create_nullable_record<kind::character>(expected_text), result[0])
        << "Failed query: " << query;
}
TEST_F(function_upper_test, binary) {
    // upper case
    // 41 42 43 44 45 46 47 48 49 4a 4b 4c 4d 4e 4f 50 51 52 53 54 55 56 57 58 59 5a
    // lower case
    // 61 62 63 64 65 66 67 68 69 6a 6b 6c 6d 6e 6f 70 71 72 73 74 75 76 77 78 79 7a

    execute_statement("create table t (c0 binary(60))");
    execute_statement("insert into t values "
                      "('"
                      "406162636465666768696a6b6c6d6e6f707172737475767778797a604142434445464748494a"
                      "4b4c4d4e4f505152535455565758595a')");
    std::string query = std::string("SELECT upper(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_text(
        "\x40\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50\x51\x52\x53\x54\x55"
        "\x56\x57\x58\x59\x5a\x60\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50"
        "\x51\x52\x53\x54\x55\x56\x57\x58\x59\x5a\x00\x00\x00\x00\x00\x00");
    EXPECT_EQ(create_nullable_record<kind::octet>(accessor::binary{expected_text}), result[0])
        << "Failed query: " << query;
}

TEST_F(function_upper_test, varbinary) {

    execute_statement("create table t (c0 varbinary(60))");
    execute_statement("insert into t values "
                      "('"
                      "406162636465666768696a6b6c6d6e6f707172737475767778797a604142434445464748494a"
                      "4b4c4d4e4f505152535455565758595a')");
    std::string query = std::string("SELECT upper(c0) FROM t");
    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query;
    accessor::binary expected_text(
        "\x40\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50\x51\x52\x53\x54\x55"
        "\x56\x57\x58\x59\x5a\x60\x41\x42\x43\x44\x45\x46\x47\x48\x49\x4a\x4b\x4c\x4d\x4e\x4f\x50"
        "\x51\x52\x53\x54\x55\x56\x57\x58\x59\x5a");
    EXPECT_EQ(create_nullable_record<kind::octet>(accessor::binary{expected_text}), result[0])
        << "Failed query: " << query;
}

TEST_F(function_upper_test, null) {
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(70))");
    execute_statement("insert into t values ('XYZ')");
    test_stmt_err("SELECT upper(null) FROM t", error_code::symbol_analyze_exception);
}

}  // namespace jogasaki::testing
