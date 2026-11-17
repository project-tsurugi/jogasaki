/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include <unordered_map>
#include <gtest/gtest.h>

#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/plan/mirror_container.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/status.h>

#include "../api/api_test_base.h"

namespace jogasaki::plan {

using namespace std::string_view_literals;

/**
 * @brief tests for the flag whether the statement reads the tables written by the statement itself
 * @details the flag is used not to use iscan and not to give TOP-N runtime features for such statements
 * (tsurugi-issues#1491, #1589)
 */
class plan_reads_written_tables_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
        execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
        execute_statement("CREATE INDEX t_c1 ON t (c1)");
        execute_statement("CREATE TABLE d (c0 INT PRIMARY KEY, c1 INT)");
    }

    void TearDown() override {
        db_teardown();
    }

    bool reads_written_tables(std::string_view sql) {
        api::statement_handle prepared{};
        std::unordered_map<std::string, api::field_type_kind> variables{};
        EXPECT_EQ(status::ok, db_->prepare(sql, variables, prepared));
        auto ret = reinterpret_cast<api::impl::prepared_statement*>(prepared.get())  //NOLINT
            ->body()->mirrors()->reads_written_tables();
        EXPECT_EQ(status::ok, db_->destroy_statement(prepared));
        return ret;
    }
};

TEST_F(plan_reads_written_tables_test, query) {
    EXPECT_FALSE(reads_written_tables("SELECT * FROM t WHERE c1 > 0 LIMIT 3"));
}

TEST_F(plan_reads_written_tables_test, insert_values) {
    EXPECT_FALSE(reads_written_tables("INSERT INTO t VALUES (1, 1)"));
}

TEST_F(plan_reads_written_tables_test, insert_select_same_table) {
    EXPECT_TRUE(reads_written_tables("INSERT INTO t SELECT c0 + 10, c1 FROM t WHERE c1 > 0 LIMIT 3"));
}

TEST_F(plan_reads_written_tables_test, insert_select_same_table_secondary_index) {
    // the secondary index of the written table is read
    EXPECT_TRUE(reads_written_tables("INSERT INTO t SELECT c0 + 10, c1 FROM t WHERE c1 = 1"));
}

TEST_F(plan_reads_written_tables_test, insert_select_other_table) {
    EXPECT_FALSE(reads_written_tables("INSERT INTO d SELECT * FROM t WHERE c1 > 0 LIMIT 3"));
}

TEST_F(plan_reads_written_tables_test, insert_select_other_table_with_subquery) {
    // the subquery reads the written table
    EXPECT_TRUE(reads_written_tables("INSERT INTO d SELECT * FROM t WHERE c0 IN (SELECT c0 FROM d)"));
}

TEST_F(plan_reads_written_tables_test, update) {
    EXPECT_TRUE(reads_written_tables("UPDATE t SET c1 = 1 WHERE c1 > 0"));
}

TEST_F(plan_reads_written_tables_test, delete) {
    EXPECT_TRUE(reads_written_tables("DELETE FROM t WHERE c1 > 0"));
}

}  // namespace jogasaki::plan
