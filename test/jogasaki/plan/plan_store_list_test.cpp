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
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/plan/statement_work_level.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/storage/storage_manager.h>
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

storage::storage_list_view get_storage_list(api::statement_handle stmt) {
    return reinterpret_cast<api::impl::prepared_statement *>(stmt.get())->body()->mirrors()->storage_list();
}

// TODO do not depends on compiler to create dag
class plan_store_list_test :
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

    std::vector<std::string> calculate_statement_store_list(std::string_view sql) {
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
        auto list = get_storage_list(prepared);
        std::vector<std::string> ret{};
        ret.reserve(list.size());
        for(auto&& l : list.entity()) {
            auto s = global::storage_manager()->find_entry(l);
            [&]() { ASSERT_TRUE(s); }();
            ret.emplace_back(s->name());
        }
        std::sort(ret.begin(), ret.end());
        [&]() { ASSERT_EQ(status::ok, db_->destroy_statement(prepared)); }();
        return ret;
    }
};

using namespace std::string_view_literals;

TEST_F(plan_store_list_test, insert) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ((std::vector<std::string>{"T"}),
            calculate_statement_store_list("INSERT INTO T (C0, C1) VALUES (1,1)"));
}

TEST_F(plan_store_list_test, insert_from_select) {
    execute_statement("CREATE TABLE T0 (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("CREATE TABLE T1 (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ((std::vector<std::string>{"T0", "T1"}),
            calculate_statement_store_list("INSERT INTO T0 SELECT * FROM T1"));
}

TEST_F(plan_store_list_test, ddl) {
    ASSERT_EQ((std::vector<std::string>{}),
            calculate_statement_store_list("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)"));
}

TEST_F(plan_store_list_test, find_op) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ((std::vector<std::string>{"T"}),
            calculate_statement_store_list("SELECT * FROM T WHERE C0=1"));
}

TEST_F(plan_store_list_test, scan_op) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ((std::vector<std::string>{"T"}),
            calculate_statement_store_list("SELECT * FROM T"));
}

TEST_F(plan_store_list_test, join) {
    execute_statement("CREATE TABLE T1 (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("CREATE TABLE T2 (C0 INT PRIMARY KEY, C1 INT)");
    ASSERT_EQ((std::vector<std::string>{"T1", "T2"}),
            calculate_statement_store_list("SELECT * FROM T1, T2"));
}

TEST_F(plan_store_list_test, join_find) {
execute_statement("CREATE TABLE t0 (c0 int)");
execute_statement("CREATE TABLE t1 (c0 int primary key, c1 int)");
    ASSERT_EQ((std::vector<std::string>{"t0", "t1"}),
            calculate_statement_store_list("SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0"));
}

TEST_F(plan_store_list_test, join_scan) {
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    ASSERT_EQ((std::vector<std::string>{"t0", "t1"}),
            calculate_statement_store_list("SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0"));
}

}
