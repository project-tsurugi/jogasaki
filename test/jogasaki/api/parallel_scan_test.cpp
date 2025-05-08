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

using takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class parallel_scan_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->scan_default_parallel(5);
        cfg->key_distribution(key_distribution_kind::uniform);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(parallel_scan_test, simple) {
    // manually verify the log message and check pivots for stored data
    execute_statement("CREATE TABLE t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (100), (200), (300)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    std::sort(result.begin(), result.end());
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4>(200)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4>(300)), result[2]);
}

TEST_F(parallel_scan_test, empty_table) {
    // verify no crash when table is empty for parallel scan
    execute_statement("CREATE TABLE t (c0 int primary key)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    ASSERT_EQ(0, result.size());
}

TEST_F(parallel_scan_test, negative_values) {
    // test with negative values
    execute_statement("CREATE TABLE t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (-100),(-200)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    std::sort(result.begin(), result.end());
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(-200)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4>(-100)), result[1]);
}

TEST_F(parallel_scan_test, various_types) {
    // test with various types and check no crash
    execute_statement("CREATE TABLE t (c0 int, c1 bigint, c2 char(20), c3 varchar(20), c4 real, c5 double, c6 decimal(5,3), primary key(c0, c1, c2, c3, c4, c5, c6))");
    execute_statement("INSERT INTO t VALUES (-1, -1, '', '', -1.0, -1.0, -1)");
    execute_statement("INSERT INTO t VALUES (10, 10, '11111111111111111111', '11111111111111111111', 10.0, 10.0, 10)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    ASSERT_EQ(2, result.size());
}

TEST_F(parallel_scan_test, multiple_pivots) {
    // manually check 10 records are picked by different scan strand
    execute_statement("CREATE TABLE t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (10),(20),(30),(40),(50),(60),(70),(80),(90),(100)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    ASSERT_EQ(10, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4>(10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4>(20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4>(30)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4>(40)), result[3]);
    EXPECT_EQ((create_nullable_record<kind::int4>(50)), result[4]);
    EXPECT_EQ((create_nullable_record<kind::int4>(60)), result[5]);
    EXPECT_EQ((create_nullable_record<kind::int4>(70)), result[6]);
    EXPECT_EQ((create_nullable_record<kind::int4>(80)), result[7]);
    EXPECT_EQ((create_nullable_record<kind::int4>(90)), result[8]);
    EXPECT_EQ((create_nullable_record<kind::int4>(100)), result[9]);
}
/**
 * @brief Tests the functionality of parallel scanning with zero pivots.
 *
 * This test verifies that the parallel scan still functions correctly
 * when `key_distribution::compute_pivots` does not return any pivots.
 *
 * - Ensures that a simple table with one record is correctly counted.
 * - Covers issue #1180.
 *
 */
TEST_F(parallel_scan_test, count_rtx_parallel_pivot_0) {
    // issues #1180
    execute_statement("CREATE TABLE t (c0 int primary key)");
    std::ostringstream query;
    query << "insert into t values ";
    auto cfg = std::make_shared<configuration>();
    cfg->scan_default_parallel(1);
    cfg->key_distribution(key_distribution_kind::uniform);
    global::config_pool(cfg);
    for (int i = 1; i <= 1; ++i) {
        if (i > 1) { query << ", "; }
        query << "(" << i << ")";
    }
    execute_statement(query.str());
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    std::string query2 = std::string("SELECT COUNT(c0) FROM t");
    execute_query(query2, *tx, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query2;
    // NOT 1001
    EXPECT_EQ((create_nullable_record<kind::int8>(1)), result[0]) << "Failed query: " << query2;
    ASSERT_EQ(status::ok, tx->commit());
}
/**
 * @brief Tests the functionality of parallel scanning when only one pivot is returned.
 *
 * This test verifies that the parallel scan works correctly even when
 * `key_distribution::compute_pivots` returns only a single pivot.
 *
 * - Ensures that the scan still returns the correct count with minimal parallelization.
 * - Covers issue #1180.
 *
 */
TEST_F(parallel_scan_test, count_rtx_parallel_pivot_1) {
    execute_statement("CREATE TABLE t (c0 int primary key)");
    std::ostringstream query;
    query << "insert into t values ";
    auto cfg = std::make_shared<configuration>();
    cfg->scan_default_parallel(2);
    cfg->key_distribution(key_distribution_kind::uniform);
    global::config_pool(cfg);
    for (int i = 1; i <= 1000; ++i) {
        if (i > 1) { query << ", "; }
        query << "(" << i << ")";
    }
    execute_statement(query.str());
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    std::string query2 = std::string("SELECT COUNT(c0) FROM t");
    execute_query(query2, *tx, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query2;
    // NOT 1001
    EXPECT_EQ((create_nullable_record<kind::int8>(1000)), result[0]) << "Failed query: " << query2;
    ASSERT_EQ(status::ok, tx->commit());
}
/**
 * @brief Original test for parallel scanning with four pivots (#1180).
 *
 * This test serves as the **original** verification for issue #1180,
 * ensuring that the parallel scan correctly handles the case where
 * `key_distribution::compute_pivots` returns four pivots.
 *
 * - It is **the base test case for #1180**, establishing correctness for parallel scan.
 * - Ensures that increasing the number of pivots does not affect correctness.
 *
 */
TEST_F(parallel_scan_test, count_rtx_parallel_pivot_3) {
    execute_statement("CREATE TABLE t (c0 int primary key)");
    std::ostringstream query;
    query << "insert into t values ";
    auto cfg = std::make_shared<configuration>();
    cfg->scan_default_parallel(4);
    cfg->key_distribution(key_distribution_kind::uniform);
    global::config_pool(cfg);
    for (int i = 1; i <= 1000; ++i) {
        if (i > 1) { query << ", "; }
        query << "(" << i << ")";
    }
    execute_statement(query.str());
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    std::string query2 = std::string("SELECT COUNT(c0) FROM t");
    execute_query(query2, *tx, result);
    ASSERT_EQ(1, result.size()) << "Query failed: " << query2;
    // NOT 1001
    EXPECT_EQ((create_nullable_record<kind::int8>(1000)), result[0]) << "Failed query: " << query2;
    ASSERT_EQ(status::ok, tx->commit());
}
} // namespace jogasaki::testing
