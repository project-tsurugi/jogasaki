/*
 * Copyright 2018-2023 Project Tsurugi.
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

using takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_long_length_varlen_data_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->prepare_test_tables(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(sql_long_length_varlen_data_test, create_table_varchar_value_max_len) {
    execute_statement("CREATE TABLE t (c0 bigint primary key, c1 varchar(2097132))"); // 2 * 1024 * 1024 - 20
}

TEST_F(sql_long_length_varlen_data_test, create_table_char_value_max_len) {
    execute_statement("CREATE TABLE t (c0 bigint primary key, c1 char(2097132))");
}

TEST_F(sql_long_length_varlen_data_test, create_table_varbinary_value_max_len) {
    execute_statement("CREATE TABLE t (c0 bigint primary key, c1 varbinary(2097132))");
}

TEST_F(sql_long_length_varlen_data_test, create_table_binary_value_max_len) {
    execute_statement("CREATE TABLE t (c0 bigint primary key, c1 binary(2097132))");
}

TEST_F(sql_long_length_varlen_data_test, create_table_varlen_value_exceeding_max) {
    test_stmt_err("CREATE TABLE t (c0 bigint primary key, c1 varchar(2097133))", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("CREATE TABLE t (c0 bigint primary key, c1 char(2097133))", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("CREATE TABLE t (c0 bigint primary key, c1 varbinary(2097133))", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("CREATE TABLE t (c0 bigint primary key, c1 binary(2097133))", error_code::unsupported_runtime_feature_exception);
}

TEST_F(sql_long_length_varlen_data_test, create_table_varbinary_as_key) {
    test_stmt_err("CREATE TABLE t (c0 varbinary(3) primary key)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(sql_long_length_varlen_data_test, create_table_varchar_longest_key) {
    execute_statement("CREATE TABLE t (c0 varchar(30716) primary key)");
    std::string long_str(30716UL, '1');
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character},
    };
    auto ps = api::create_parameter_set();
    ps->set_character("p0", long_str);
    execute_statement("INSERT INTO t VALUES (:p0)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((typed_nullable_record<kind::character>(
            std::tuple{character_type(true, 30716)},
            std::tuple{accessor::text{long_str}}, {false})), result[0]);
    }
}

TEST_F(sql_long_length_varlen_data_test, create_table_binary_longest_key) {
    execute_statement("CREATE TABLE t (c0 binary(30716) primary key)");
    std::string long_str(30716UL, '\1');
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::octet},
    };
    auto ps = api::create_parameter_set();
    ps->set_character("p0", long_str);
    execute_statement("INSERT INTO t VALUES (:p0)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((typed_nullable_record<kind::octet>(
            std::tuple{octet_type(false, 30716)},
            std::tuple{accessor::binary{long_str}}, {false})), result[0]);
    }
}

TEST_F(sql_long_length_varlen_data_test, create_table_varlen_as_key_exceeding_max) {
    test_stmt_err("CREATE TABLE t (c0 binary(30717) primary key)", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("CREATE TABLE t (c0 varchar(30717) primary key)", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("CREATE TABLE t (c0 char(30717) primary key)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(sql_long_length_varlen_data_test, create_table_longest_value) {
    execute_statement("create table t (c0 int primary key, c1 varchar(*))");
    std::string long_str(2*1024*1024UL - 20UL, '0');
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character},
    };
    auto ps = api::create_parameter_set();
    ps->set_character("p0", long_str);
    execute_statement("INSERT INTO t VALUES (0, :p0)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::character>(std::tuple{accessor::text{long_str}}, {false})), result[0]);
    }
}

TEST_F(sql_long_length_varlen_data_test, too_long_varbinary_data) {
    execute_statement("create table t (c0 int primary key, c1 varbinary(*))");
    // page pool page size is 2MB, so maximum boundary exists around it, but it's not investigated yet // TODO
    std::string long_str(2*1024*1024UL - 20UL, '\1');
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::octet},
    };
    auto ps = api::create_parameter_set();
    ps->set_character("p0", long_str);
    execute_statement("INSERT INTO t VALUES (0, :p0)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::octet>(std::tuple{accessor::binary{long_str}}, {false})), result[0]);
    }
}

}
