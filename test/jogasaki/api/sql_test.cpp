/*
 * Copyright 2018-2020 tsurugi project.
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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class sql_test :
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
        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(sql_test, cross_join) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "INSERT INTO T10 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T10 (C0, C1) VALUES (4, 40.0)");
    execute_statement( "INSERT INTO T10 (C0, C1) VALUES (5, 50.0)");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0, T10", result);
    ASSERT_EQ(6, result.size());
}

TEST_F(sql_test, update_by_part_of_primary_key) {
    execute_statement( "INSERT INTO T20 (C0, C2, C4) VALUES (1, 100.0, '111')");
    execute_statement( "UPDATE T20 SET C2=200.0 WHERE C0=1");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1, C2 FROM T20", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_EQ(1, rec.get_value<std::int64_t>(0));
    EXPECT_TRUE(rec.is_null(1));
    EXPECT_DOUBLE_EQ(200.0, rec.get_value<double>(2));
    EXPECT_FALSE(rec.is_null(2));
}

TEST_F(sql_test, update_primary_key) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    execute_statement( "UPDATE T0 SET C0=3, C1=30.0 WHERE C1=10.0");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0, C1 FROM T0 ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    auto meta = result[0].record_meta();
    EXPECT_EQ(2, result[0].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(20.0, result[0].get_value<double>(1));
    EXPECT_EQ(3, result[1].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(30.0, result[1].get_value<double>(1));
}

TEST_F(sql_test, count_empty_records) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, sum_empty_records) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, count_null) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, sum_null) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, count_distinct) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(2, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, count_distinct_empty) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, count_distinct_null) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(distinct C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, count_rows) {
    execute_statement( "INSERT INTO T0 (C0) VALUES (1)");
    execute_statement( "INSERT INTO T0 (C0) VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(*) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(2, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, max) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C0), MAX(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_FALSE(rec.is_null(1));
    EXPECT_EQ(3, rec.get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(30.0, rec.get_value<double>(1));
}

TEST_F(sql_test, min) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (3, 30.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MIN(C0), MIN(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_FALSE(rec.is_null(1));
    EXPECT_EQ(1, rec.get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(10.0, rec.get_value<double>(1));
}

TEST_F(sql_test, count_rows_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT COUNT(*) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));
    EXPECT_EQ(0, rec.get_value<std::int64_t>(0));
}

TEST_F(sql_test, sum_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT SUM(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, avg_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT AVG(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, max_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MAX(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

TEST_F(sql_test, min_empty_table) {
    std::vector<mock::basic_record> result{};
    execute_query("SELECT MIN(C1) FROM T0", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_TRUE(rec.is_null(0));
}

}
