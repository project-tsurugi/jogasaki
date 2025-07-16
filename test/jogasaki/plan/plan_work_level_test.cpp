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

#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <unordered_map>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/plan/statement_work_level.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>

#include "../api/api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;

using kind = meta::field_type_kind;

plan::statement_work_level get_work(api::statement_handle stmt) {
    return reinterpret_cast<api::impl::prepared_statement *>(stmt.get())->body()->mirrors()->work_level();
}

// TODO do not depends on compiler to create dag
class plan_work_level_test :
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

    plan::statement_work_level calculate_statement_work(std::string_view sql) {
        api::statement_handle prepared{};
        std::unordered_map<std::string, api::field_type_kind> variables{};
        [&]() { ASSERT_EQ(status::ok, db_->prepare(sql, variables, prepared)); }();
        if (to_explain()) {
            std::unique_ptr<api::executable_statement> stmt{};
            api::impl::parameter_set params{};
            [&]() { ASSERT_EQ(status::ok, db_->resolve(prepared, maybe_shared_ptr{&params}, stmt)); }();
            db_->explain(*stmt, std::cout);
            std::cout << std::endl;
        }
        auto ret = get_work(prepared);
        [&]() { ASSERT_EQ(status::ok, db_->destroy_statement(prepared)); }();
        return ret;
    }
};

using namespace std::string_view_literals;

TEST_F(plan_work_level_test, insert) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ(plan::statement_work_level_kind::simple_write,
            calculate_statement_work("INSERT INTO T (C0, C1) VALUES (1,1)").kind());
}

TEST_F(plan_work_level_test, ddl) {
    ASSERT_EQ(plan::statement_work_level_kind::infinity,
            calculate_statement_work("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)").kind());
}

TEST_F(plan_work_level_test, key_operation) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ(plan::statement_work_level_kind::key_operation,
            calculate_statement_work("SELECT * FROM T WHERE C0=1").kind());
}

TEST_F(plan_work_level_test, simple_crud_with_filter) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ(plan::statement_work_level_kind::simple_crud,
            calculate_statement_work("SELECT * FROM T WHERE C0=1 AND C1=1").kind());
}

TEST_F(plan_work_level_test, simple_crud_with_secondary_index) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY NOT NULL, C1 INT NOT NULL)");
    execute_statement("CREATE INDEX I2 ON T(C1)");
    ASSERT_EQ(plan::statement_work_level_kind::simple_crud,
            calculate_statement_work("SELECT * FROM T WHERE C1=1").kind());
}

// UNION/LIMIT is not supported yet
TEST_F(plan_work_level_test, DISABLED_simple_multirecord_operation) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ(plan::statement_work_level_kind::simple_multirecord_operation,
            calculate_statement_work("SELECT C1 FROM T WHERE C0=1 LIMIT 1").kind());
}

TEST_F(plan_work_level_test, join) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ(plan::statement_work_level_kind::join,
            calculate_statement_work("SELECT * FROM T T1, T T2 WHERE T1.C0=1 AND T2.C0=1 AND T1.C0=T2.C0").kind());
}

TEST_F(plan_work_level_test, aggregate) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ(plan::statement_work_level_kind::aggregate,
            calculate_statement_work("SELECT SUM(C1) FROM T WHERE C0=1").kind());
}
}
