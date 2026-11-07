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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/scan_impl_kind.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;
using namespace std::string_view_literals;

using kind = meta::field_type_kind;

/**
 * @brief tests for the scan with row count (tsurugi-issues#1589 R2)
 * @details the parameters are (enable_scan_limit, scan_impl).
 */
class sql_scan_limit_test :
    public ::testing::TestWithParam<std::tuple<bool, scan_impl_kind>>,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_scan_limit(scan_limit_enabled());
        cfg->scan_impl(std::get<1>(GetParam()));
        // enable rtx parallel scan to verify the scan with row count is not split
        cfg->scan_default_parallel(4);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

    bool scan_limit_enabled() {
        return std::get<0>(GetParam());
    }

    // returns whether the plan has the scan with row count
    bool has_scan_with_count(std::string_view query) {
        std::string plan{};
        explain_statement(query, plan);
        // looks into the scan operator object, which has row_slice before its input_ports
        auto begin = plan.find(R"("kind":"scan")");
        if(begin == std::string::npos) {
            return false;
        }
        auto end = plan.find(R"("input_ports")", begin);
        auto scan = std::string_view{plan}.substr(begin, end - begin);
        return contains(scan, R"("row_slice":{"count")");
    }

    std::vector<std::int32_t> query_c0(std::string_view query, bool rtx = false) {
        std::vector<mock::basic_record> result{};
        if(rtx) {
            auto tx = utils::create_transaction(*db_, true, false);
            execute_query(query, *tx, result);
            EXPECT_EQ(status::ok, tx->commit());
        } else {
            execute_query(query, result);
        }
        std::vector<std::int32_t> ret{};
        ret.reserve(result.size());
        for(auto&& r : result) {
            ret.emplace_back(r.get_value<std::int32_t>(0));
        }
        return ret;
    }

    static bool contains(std::string_view whole, std::string_view part) {
        return whole.find(part) != std::string_view::npos;
    }

    void insert_rows(std::size_t count) {
        for(std::size_t i = 0; i < count; ++i) {
            execute_statement(
                "INSERT INTO t VALUES (" + std::to_string(i) + ", " + std::to_string(count - i) + ")"
            );
        }
    }
};

INSTANTIATE_TEST_SUITE_P(
    scan_limit,
    sql_scan_limit_test,
    ::testing::Values(
        std::make_tuple(true, scan_impl_kind::automatic),
        std::make_tuple(true, scan_impl_kind::legacy),
        std::make_tuple(true, scan_impl_kind::iscan),
        std::make_tuple(false, scan_impl_kind::automatic)
    ),
    [](auto const& info) {
        if(! std::get<0>(info.param)) {
            return std::string{"scan_limit_disabled"};
        }
        auto impl = std::get<1>(info.param);
        return std::string{impl == scan_impl_kind::automatic ? "auto" : to_string_view(impl)};
    }
);

TEST_P(sql_scan_limit_test, primary_order_by_limit) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    insert_rows(10);
    auto query = "SELECT c0 FROM t ORDER BY c0 LIMIT 3";
    EXPECT_EQ(scan_limit_enabled(), has_scan_with_count(query));
    EXPECT_EQ((std::vector<std::int32_t>{0, 1, 2}), query_c0(query));
}

TEST_P(sql_scan_limit_test, primary_order_by_limit_rtx) {
    // the scan with row count is not split even if rtx parallel scan is enabled
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    insert_rows(100);
    auto query = "SELECT c0 FROM t ORDER BY c0 LIMIT 3";
    EXPECT_EQ((std::vector<std::int32_t>{0, 1, 2}), query_c0(query, true));
}

TEST_P(sql_scan_limit_test, limit_without_order_by) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    insert_rows(10);
    auto query = "SELECT c0 FROM t LIMIT 3";
    EXPECT_EQ(scan_limit_enabled(), has_scan_with_count(query));
    EXPECT_EQ(3, query_c0(query).size());
    EXPECT_EQ(3, query_c0(query, true).size());
}

TEST_P(sql_scan_limit_test, limit_zero) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    insert_rows(10);
    EXPECT_EQ(0, query_c0("SELECT c0 FROM t ORDER BY c0 LIMIT 0").size());
    EXPECT_EQ(0, query_c0("SELECT c0 FROM t LIMIT 0").size());
}

TEST_P(sql_scan_limit_test, limit_exceeding_rows) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    insert_rows(3);
    EXPECT_EQ((std::vector<std::int32_t>{0, 1, 2}), query_c0("SELECT c0 FROM t ORDER BY c0 LIMIT 10"));
}

TEST_P(sql_scan_limit_test, range_order_by_limit) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    insert_rows(10);
    auto query = "SELECT c0 FROM t WHERE c0 > 4 ORDER BY c0 LIMIT 2";
    EXPECT_EQ(scan_limit_enabled(), has_scan_with_count(query));
    EXPECT_EQ((std::vector<std::int32_t>{5, 6}), query_c0(query));
}

TEST_P(sql_scan_limit_test, secondary_order_by_limit) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1)");
    insert_rows(10);
    // c1 is 10 - c0
    auto query = "SELECT c0 FROM t ORDER BY c1 LIMIT 3";
    EXPECT_EQ(scan_limit_enabled(), has_scan_with_count(query));
    EXPECT_EQ((std::vector<std::int32_t>{9, 8, 7}), query_c0(query));
    EXPECT_EQ((std::vector<std::int32_t>{9, 8, 7}), query_c0(query, true));
}

TEST_P(sql_scan_limit_test, deleted_rows_are_not_counted) {
    // rows deleted (but possibly remaining as tombstones) at the beginning of the range are not counted
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t(c1)");
    insert_rows(10);
    execute_statement("DELETE FROM t WHERE c0 < 5");
    EXPECT_EQ((std::vector<std::int32_t>{5, 6, 7}), query_c0("SELECT c0 FROM t ORDER BY c0 LIMIT 3"));
    EXPECT_EQ((std::vector<std::int32_t>{5, 6, 7}), query_c0("SELECT c0 FROM t ORDER BY c0 LIMIT 3", true));
    // c1 is 10 - c0, so the deleted rows are at the end of secondary index
    execute_statement("DELETE FROM t WHERE c0 > 7");
    EXPECT_EQ((std::vector<std::int32_t>{7, 6}), query_c0("SELECT c0 FROM t ORDER BY c1 LIMIT 2"));
}

TEST_P(sql_scan_limit_test, deleted_rows_in_same_transaction) {
    // rows deleted by the transaction itself are not counted
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    insert_rows(10);
    auto tx = utils::create_transaction(*db_);
    execute_statement("DELETE FROM t WHERE c0 < 5", *tx);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c0 FROM t ORDER BY c0 LIMIT 3", *tx, result);
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(5)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4>(6)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4>(7)), result[2]);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_P(sql_scan_limit_test, insert_select_with_limit) {
    // the statement with write operations is compiled without scan_limit (see tsurugi-issues#1491)
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    insert_rows(10);
    EXPECT_FALSE(has_scan_with_count("INSERT INTO t SELECT c0 + 100, c1 FROM t ORDER BY c0 LIMIT 3"));
    EXPECT_FALSE(has_scan_with_count("INSERT INTO t SELECT c0 + 100, c1 FROM t LIMIT 3"));
    execute_statement("INSERT INTO t SELECT c0 + 100, c1 FROM t ORDER BY c0 LIMIT 3");
    EXPECT_EQ(
        (std::vector<std::int32_t>{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 100, 101, 102}),
        query_c0("SELECT c0 FROM t ORDER BY c0")
    );
}

TEST_P(sql_scan_limit_test, insert_select_into_same_table_head) {
    // inserting rows ahead of the scan cursor must not be picked up by the scan
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    insert_rows(3);
    execute_statement("INSERT INTO t SELECT c0 + 1000, c1 FROM t LIMIT 5");
    EXPECT_EQ((std::vector<std::int32_t>{0, 1, 2, 1000, 1001, 1002}), query_c0("SELECT c0 FROM t ORDER BY c0"));
}

TEST_P(sql_scan_limit_test, insert_select_into_tombstones_ahead) {
    // regression test for Halloween problem (tsurugi-issues#1491, #1492)
    // inserting into the tombstones ahead of the scan cursor must not be picked up by the scan
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    // keep the long transaction open to prevent tombstones from being garbage-collected
    auto gc_block = utils::create_transaction(*db_, false, true);
    execute_statement("INSERT INTO t VALUES (0, 0), (5, 0), (10, 0), (15, 0), (20, 0)");
    execute_statement("DELETE FROM t WHERE c0 = 5 OR c0 = 15");
    execute_statement("INSERT INTO t SELECT c0 + 5, c1 FROM t LIMIT 3");
    EXPECT_EQ((std::vector<std::int32_t>{0, 5, 10, 15, 20, 25}), query_c0("SELECT c0 FROM t ORDER BY c0"));
    ASSERT_EQ(status::ok, gc_block->abort());
}

}  // namespace jogasaki::testing
