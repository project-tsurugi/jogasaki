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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

// testcase to limit the number of result set writers by configuration::max_result_set_writers
// We verify the main functionality automatically by checking the result records.
// But verifying the timing dependent scenarios need to be done manually by checking the log message with writer_pool:
//
//   writer_pool::acquire() success
//   writer_pool::release() success
//   writer_pool::acquire() failed, yielding task
class limit_resultset_writers_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->busy_worker(true);  // enabled in order to make multiple emit tasks run concurrently
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(limit_resultset_writers_test, simple_query_with_max_writers_1) {
    // test that simple queries work normally with max_result_set_writers = 1
    // with default_partitions=5 (by default), some partitions fail to acquire, so
    global::config_pool()->max_result_set_writers(2);
    execute_statement("create table t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (1), (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t ORDER BY c0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[1]);
    }
}

TEST_F(limit_resultset_writers_test, simple_query_with_max_writers_default_partitions_1) {
    // test that simple queries work normally with both max_result_set_writers and default_partitions to be 1
    // check manually that yield doesn't occur since only one partition requires a writer at a time.
    global::config_pool()->max_result_set_writers(1);
    global::config_pool()->default_partitions(1);
    execute_statement("create table t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (1), (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t ORDER BY c0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[1]);
    }
}

TEST_F(limit_resultset_writers_test, union_all_with_max_writers_1) {
    // test that UNION ALL works even when max_result_set_writers is set to 1
    global::config_pool()->max_result_set_writers(1);
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("create table t1 (c0 int primary key)");
    execute_statement("INSERT INTO t0 VALUES (1), (2)");
    execute_statement("INSERT INTO t1 VALUES (3), (4)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t0 UNION ALL SELECT c0 FROM t1", result);
        ASSERT_EQ(4, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[2]);
        EXPECT_EQ((create_nullable_record<kind::int4>(4)), result[3]);
    }
}

TEST_F(limit_resultset_writers_test, union_all_three_tables_with_max_writers_1) {
    // test UNION ALL with 3 tables to ensure degraded operation works with multiple writers
    global::config_pool()->max_result_set_writers(1);
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("create table t1 (c0 int primary key)");
    execute_statement("create table t2 (c0 int primary key)");
    execute_statement("INSERT INTO t0 VALUES (1), (2)");
    execute_statement("INSERT INTO t1 VALUES (3), (4)");
    execute_statement("INSERT INTO t2 VALUES (5), (6)");
    {
        std::vector<mock::basic_record> result{};
        execute_query(
            "SELECT c0 FROM t0 "
            "UNION ALL SELECT c0 FROM t1 "
            "UNION ALL SELECT c0 FROM t2",
            result
        );
        ASSERT_EQ(6, result.size());
        std::sort(result.begin(), result.end());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[2]);
        EXPECT_EQ((create_nullable_record<kind::int4>(4)), result[3]);
        EXPECT_EQ((create_nullable_record<kind::int4>(5)), result[4]);
        EXPECT_EQ((create_nullable_record<kind::int4>(6)), result[5]);
    }
}

TEST_F(limit_resultset_writers_test, join_query_with_max_writers_1) {
    // test that join queries work with max_result_set_writers = 1
    global::config_pool()->max_result_set_writers(1);
    execute_statement("create table t0 (c0 int primary key, c1 int)");
    execute_statement("create table t1 (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (2, 20)");
    execute_statement("INSERT INTO t1 VALUES (1, 100), (2, 200)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT t0.c0, t0.c1, t1.c1 FROM t0, t1 WHERE t0.c0 = t1.c0 ORDER BY t0.c0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 10, 100)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4>(2, 20, 200)), result[1]);
    }
}

TEST_F(limit_resultset_writers_test, union_all_with_30_records) {
    // test UNION ALL with approximately 30 records to verify pool behavior with multiple records
    global::config_pool()->max_result_set_writers(2);
    execute_statement("create table t0 (c0 int primary key)");
    execute_statement("create table t1 (c0 int primary key)");

    // insert 15 records into t0 using a loop
    for (std::int32_t i = 1; i <= 15; ++i) {
        std::string sql = std::string("INSERT INTO t0 VALUES (") + std::to_string(i) + ")";
        execute_statement(sql);
    }

    // insert 15 records into t1 using a loop
    for (std::int32_t i = 16; i <= 30; ++i) {
        std::string sql = std::string("INSERT INTO t1 VALUES (") + std::to_string(i) + ")";
        execute_statement(sql);
    }

    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t0 UNION ALL SELECT c0 FROM t1", result);
        ASSERT_EQ(30, result.size());
        std::sort(result.begin(), result.end());

        // verify all 30 records
        for (std::int32_t i = 1; i <= 30; ++i) {
            EXPECT_EQ((create_nullable_record<kind::int4>(i)), result[i - 1]);
        }
    }
}

}
