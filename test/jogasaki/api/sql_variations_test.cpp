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

#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_variations_test :
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

using namespace std::string_view_literals;

// natural join not supported by current compiler
TEST_F(sql_variations_test, natural_join) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT0 (C0 int primary key, C1 int)");
    execute_statement("create table TT1 (C1 int primary key, C2 int)");
    execute_statement("INSERT INTO TT0 (C0, C1) VALUES (1,1)");
    execute_statement("INSERT INTO TT1 (C1, C2) VALUES (1,1)");
    test_stmt_err("select * from TT0 natural join TT1", error_code::unsupported_compiler_feature_exception);
}

TEST_F(sql_variations_test, cross_join) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table TT0 (C0 int primary key, C1 int)");
    execute_statement("create table TT1 (C1 int primary key, C2 int)");
    execute_statement("INSERT INTO TT0 (C0, C1) VALUES (1,1)");
    execute_statement("INSERT INTO TT1 (C1, C2) VALUES (1,1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from TT0, TT1", result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from TT0 cross join TT1", result);
        ASSERT_EQ(1, result.size());
    }
}

TEST_F(sql_variations_test, comment_by_two_minus) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("-- create table TT (C0 int primary key, C1 int) \ncreate table TT (C0 int primary key, C1 int)");
    execute_statement("INSERT INTO TT VALUES (1,1)");
}

TEST_F(sql_variations_test, comment_by_block) {
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create /* table */ table TT (C0 int primary key, C1 int)");
    execute_statement("INSERT INTO TT VALUES (1,1)");
}


TEST_F(sql_variations_test, empty_statements) {
    // compiler treats empty string as empty compilation units (with no statement) and runtime rejects it as unsupported
    test_stmt_err("", error_code::unsupported_runtime_feature_exception);
}

TEST_F(sql_variations_test, only_statement_separator) {
    execute_statement(";");
}

TEST_F(sql_variations_test, multiple_empty_statements) {
    execute_statement(";;;");
}

TEST_F(sql_variations_test, statement_with_semicolon) {
    execute_statement("create table t (C0 int primary key);");
    execute_statement("INSERT INTO t VALUES (0)");
}

TEST_F(sql_variations_test, empty_and_non_empty_statement) {
    execute_statement(";create table t (C0 int primary key);");
    execute_statement("INSERT INTO t VALUES (0)");
}

TEST_F(sql_variations_test, multiple_statements) {
    // jogasaki raises error if sql text has multiple statements
    execute_statement("create table t (C0 int primary key)");
    test_stmt_err("INSERT INTO t VALUES (0);INSERT INTO t VALUES (1)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(sql_variations_test, multiple_lines) {
    // jogasaki raises error if sql text has multiple statements
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table t (C0 int primary key)");
    execute_statement("INSERT\n INTO\n t \nVALUES (0)");
}

TEST_F(sql_variations_test, control_character_in_delimited_identifier) {
    execute_statement("create table \"tt\ntt\" (\"aa\nbb\" int primary key)");
    execute_statement("INSERT INTO \"tt\ntt\" (\"aa\nbb\") VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select \"aa\nbb\" from \"tt\ntt\"", result);
        ASSERT_EQ(1, result.size());
    }
}
}
