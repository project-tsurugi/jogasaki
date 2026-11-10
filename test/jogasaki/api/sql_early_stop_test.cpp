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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::mock;
using namespace std::string_view_literals;

using kind = meta::field_type_kind;

/**
 * @brief tests for the early stop by the forward exchange with row count (tsurugi-issues#1589 R3)
 * @details the parameters are (enable_early_stop, use_rtx). The limited query results without ORDER BY are verified
 * by their row count and that the rows are from the result without LIMIT.
 */
class sql_early_stop_test :
    public ::testing::TestWithParam<std::tuple<bool, bool>>,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_early_stop(std::get<0>(GetParam()));
        // enable rtx parallel scan to verify the early stop with multiple upstream tasks
        cfg->scan_default_parallel(4);
        // yield frequently to verify the early stop with yield
        cfg->scan_block_size(10);
        cfg->scan_yield_interval(0);
        db_setup(cfg);
        create_tables();
    }

    void TearDown() override {
        db_teardown();
    }

    bool use_rtx() {
        return std::get<1>(GetParam());
    }

    // t: k in 0..199, v = k % 10, w = k % 7 (w > 1 filters out 2/7 of the rows)
    // u: k in 0..99 step 2, v = k % 10
    void create_tables() {
        execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v INT, w INT)");
        execute_statement("CREATE TABLE u (k INT PRIMARY KEY, v INT)");
        execute_statement("CREATE TABLE r (k INT PRIMARY KEY, v INT)");
        std::string t_values{};
        for(std::int32_t k = 0; k < 200; ++k) {
            t_values += (k == 0 ? "" : ", ");
            t_values += "(" + std::to_string(k) + ", " + std::to_string(k % 10) + ", " + std::to_string(k % 7) + ")";
        }
        execute_statement("INSERT INTO t VALUES " + t_values);
        std::string u_values{};
        for(std::int32_t k = 0; k < 100; k += 2) {
            u_values += (k == 0 ? "" : ", ");
            u_values += "(" + std::to_string(k) + ", " + std::to_string(k % 10) + ")";
        }
        execute_statement("INSERT INTO u VALUES " + u_values);
    }

    std::vector<basic_record> query(std::string_view sql) {
        std::vector<basic_record> result{};
        if(use_rtx()) {
            auto tx = utils::create_transaction(*db_, true, false);
            execute_query(sql, *tx, result);
            EXPECT_EQ(status::ok, tx->commit());
        } else {
            execute_query(sql, result);
        }
        return result;
    }

    static std::string to_string(std::vector<basic_record> const& records) {
        std::stringstream ss{};
        for(auto&& rec : records) {
            ss << rec << "\n";
        }
        return ss.str();
    }

    // verifies the result of `candidates` with LIMIT `count` (without ORDER BY)
    void verify_limited(std::string_view candidates, std::size_t count) {
        auto sql = std::string{candidates} + " LIMIT " + std::to_string(count);
        SCOPED_TRACE(sql);
        auto all = query(candidates);
        std::sort(all.begin(), all.end());
        auto rows = query(sql);
        std::sort(rows.begin(), rows.end());
        ASSERT_EQ(std::min(count, all.size()), rows.size()) << to_string(rows);
        EXPECT_TRUE(std::includes(all.begin(), all.end(), rows.begin(), rows.end())) << to_string(rows);
    }
};

INSTANTIATE_TEST_SUITE_P(
    early_stop,
    sql_early_stop_test,
    ::testing::Combine(::testing::Bool(), ::testing::Bool()),
    [](auto const& info) {
        return std::string{std::get<0>(info.param) ? "enabled" : "disabled"} +
            (std::get<1>(info.param) ? "_rtx" : "_occ");
    }
);

TEST_P(sql_early_stop_test, filter) {
    // the scan has no row count because of the filter, and stops by the forward exchange
    verify_limited("SELECT * FROM t WHERE w > 1", 3);
    verify_limited("SELECT * FROM t WHERE w > 1", 1);
    verify_limited("SELECT * FROM t WHERE w > 1", 0);
    verify_limited("SELECT * FROM t WHERE w > 1", 100);
    verify_limited("SELECT * FROM t WHERE w > 1", 1000);
}

TEST_P(sql_early_stop_test, project_and_filter) {
    verify_limited("SELECT k + 1, v * 2 FROM t WHERE w > 1", 3);
}

TEST_P(sql_early_stop_test, index_join) {
    verify_limited("SELECT * FROM t JOIN u ON t.k = u.k WHERE t.w > 1", 3);
    verify_limited("SELECT * FROM t LEFT OUTER JOIN u ON t.k = u.k WHERE t.w > 1", 3);
    verify_limited("SELECT * FROM t WHERE EXISTS (SELECT * FROM u WHERE u.k = t.k) AND t.w > 1", 3);
    verify_limited("SELECT * FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.k = t.k) AND t.w > 1", 3);
}

TEST_P(sql_early_stop_test, cogroup_join) {
    // one row of t joins with many rows of u
    verify_limited("SELECT t.k, u.k FROM t JOIN u ON t.v = u.v", 3);
    verify_limited("SELECT t.k, u.k FROM t JOIN u ON t.v = u.v", 30);
}

TEST_P(sql_early_stop_test, group) {
    verify_limited("SELECT v, COUNT(*) FROM t GROUP BY v", 3);
    verify_limited("SELECT v, w FROM t GROUP BY v, w", 5);
}

TEST_P(sql_early_stop_test, values) {
    verify_limited("SELECT * FROM (VALUES (1), (2), (3), (4)) AS x(a)", 2);
}

TEST_P(sql_early_stop_test, union_all) {
    verify_limited("SELECT * FROM (SELECT k FROM t WHERE w > 1 UNION ALL SELECT k FROM u WHERE v > 0) x", 3);
}

TEST_P(sql_early_stop_test, scalar_subquery) {
    verify_limited("SELECT t.k, (SELECT u.v FROM u WHERE u.k = t.k) FROM t WHERE t.w > 1", 3);
}

TEST_P(sql_early_stop_test, ordered_limit) {
    // the ordered limit is not stopped early by forward (group exchange is used), but verify the result just in case
    auto rows = query("SELECT k FROM t WHERE w > 1 ORDER BY k LIMIT 3");
    ASSERT_EQ(3, rows.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(2)), rows[0]);
    EXPECT_EQ((create_nullable_record<kind::int4>(3)), rows[1]);
    EXPECT_EQ((create_nullable_record<kind::int4>(4)), rows[2]);
}

TEST_P(sql_early_stop_test, insert_select) {
    // the statement with write operations also stops early by the forward exchange
    if(use_rtx()) {
        GTEST_SKIP() << "write statement is not available in rtx";
    }
    execute_statement("INSERT INTO r SELECT k, v FROM t WHERE w > 1 LIMIT 5");
    auto rows = query("SELECT * FROM r");
    ASSERT_EQ(5, rows.size());
    auto all = query("SELECT k, v FROM t WHERE w > 1");
    std::sort(all.begin(), all.end());
    std::sort(rows.begin(), rows.end());
    EXPECT_TRUE(std::includes(all.begin(), all.end(), rows.begin(), rows.end())) << to_string(rows);
}

}  // namespace jogasaki::testing
