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
#include <cstdint>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::mock;
using namespace std::string_view_literals;

// the plan has the row count (of the scan or the forward exchange), or the scan with direction
constexpr std::string_view row_count = R"("row_slice":{"count")";
constexpr std::string_view backward = R"("direction":"backward")";
constexpr std::string_view forward = R"("direction":"forward")";

/**
 * @brief tests for TOP-N optimizations on the statements with write operations (tsurugi-issues#1589)
 * @details the same tables are created with suffix `_a` and `_b`, and the statement is executed on `_a` without
 * TOP-N runtime features and on `_b` with them. Then the table contents are compared. The TOP-N runtime features are
 * given only if the statement does not read the table written by itself, and such statement uses iscan (so the
 * backward scan works regardless of the transaction engine configuration).
 */
class sql_topn_write_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        cfg_ = std::make_shared<configuration>();
        // yield frequently to verify the order is kept across yields
        cfg_->scan_block_size(5);
        cfg_->scan_yield_interval(0);
        db_setup(cfg_);
        create_tables("_a");
        create_tables("_b");
    }

    void TearDown() override {
        db_teardown();
    }

    // t: k in 0..59, v = k * 7 % 60 (unique) with the index t_v, w = k % 7
    // d: the destination table for INSERT ... SELECT
    void create_tables(std::string_view suffix) {
        std::string s{suffix};
        execute_statement("CREATE TABLE t" + s + " (k INT PRIMARY KEY, v INT, w INT)");
        execute_statement("CREATE INDEX t" + s + "_v ON t" + s + " (v)");
        execute_statement("CREATE TABLE d" + s + " (k INT PRIMARY KEY, v INT, w INT)");
        std::string values{};
        for(std::int32_t i = 0; i < 60; ++i) {
            auto k = (i * 37) % 60;
            values += (i == 0 ? "" : ", ");
            values += "(" + std::to_string(k) + ", " + std::to_string(k * 7 % 60) + ", " + std::to_string(k % 7) + ")";
        }
        execute_statement("INSERT INTO t" + s + " VALUES " + values);
    }

    void set_topn_features(bool enabled) {
        cfg_->enable_scan_limit(enabled);
        cfg_->enable_ordered_scan(enabled);
        cfg_->enable_ordered_forward(enabled);
        cfg_->enable_backward_scan(enabled);
        cfg_->enable_apply_preserves_order(enabled);
    }

    // replaces `{}` in the query with the suffix
    static std::string with_suffix(std::string_view query, std::string_view suffix) {
        std::string ret{};
        for(std::size_t i = 0; i < query.size(); ++i) {
            if(query.substr(i, 2) == "{}") {
                ret += suffix;
                ++i;
                continue;
            }
            ret += query[i];
        }
        return ret;
    }

    std::string plan(std::string_view sql) {
        std::string ret{};
        explain_statement(sql, ret);
        return ret;
    }

    std::vector<basic_record> contents(std::string_view table) {
        std::vector<basic_record> result{};
        execute_query("SELECT * FROM " + std::string{table}, result);
        std::sort(result.begin(), result.end());
        return result;
    }

    static std::string to_string(std::vector<basic_record> const& records) {
        std::stringstream ss{};
        for(auto&& rec : records) {
            ss << rec << "\n";
        }
        return ss.str();
    }

    // executes the statement without TOP-N features on `_a` and with them on `_b`, and verifies the contents of the
    // table are the same. If expected_plan is not empty, it also verifies the plan with TOP-N features contains it
    // (but the plan can change).
    void verify(std::string_view statement, std::string_view table, std::string_view expected_plan = {}) {
        SCOPED_TRACE(statement);
        set_topn_features(true);
        if(! expected_plan.empty()) {
            auto p = plan(with_suffix(statement, "_b"));
            EXPECT_NE(std::string::npos, p.find(expected_plan)) << p;
        }
        for(auto sfx : {"_a"sv, "_b"sv}) {
            execute_statement("DELETE FROM d" + std::string{sfx});
        }
        set_topn_features(false);
        execute_statement(with_suffix(statement, "_a"));
        set_topn_features(true);
        execute_statement(with_suffix(statement, "_b"));
        auto expected = contents(with_suffix(table, "_a"));
        auto actual = contents(with_suffix(table, "_b"));
        EXPECT_FALSE(expected.empty());
        EXPECT_EQ(expected, actual) << "expected:\n" << to_string(expected) << "actual:\n" << to_string(actual);
    }

    // same as verify(), but verifies the TOP-N runtime features are not given because the statement reads the table
    // written by itself (the scan has neither direction nor row count)
    void verify_without_topn(std::string_view statement, std::string_view table) {
        SCOPED_TRACE(statement);
        set_topn_features(true);
        auto p = plan(with_suffix(statement, "_b"));
        EXPECT_EQ(std::string::npos, p.find(forward)) << p;
        EXPECT_EQ(std::string::npos, p.find(backward)) << p;
        verify(statement, table);
    }

    std::shared_ptr<configuration> cfg_{};  //NOLINT
};

TEST_F(sql_topn_write_test, insert_select) {
    verify("INSERT INTO d{} SELECT * FROM t{} ORDER BY k LIMIT 3", "d{}", row_count);
    verify("INSERT INTO d{} SELECT * FROM t{} ORDER BY k DESC LIMIT 3", "d{}", backward);
    verify("INSERT INTO d{} SELECT * FROM t{} ORDER BY v LIMIT 3", "d{}", row_count);
    verify("INSERT INTO d{} SELECT * FROM t{} LIMIT 3", "d{}", row_count);
}

TEST_F(sql_topn_write_test, insert_select_with_filter) {
    // the scan has no row count because of the filter, and the ordered forward exchange takes the first rows
    verify("INSERT INTO d{} SELECT * FROM t{} WHERE w > 1 ORDER BY k LIMIT 3", "d{}", forward);
    verify("INSERT INTO d{} SELECT * FROM t{} WHERE w > 1 ORDER BY k DESC LIMIT 3", "d{}", backward);
}

TEST_F(sql_topn_write_test, insert_select_without_limit) {
    verify("INSERT INTO d{} SELECT * FROM t{} WHERE w > 1 ORDER BY k", "d{}");
}

TEST_F(sql_topn_write_test, insert_select_other_table_with_subquery) {
    // the subquery reads the table written by the statement itself
    verify_without_topn(
        "INSERT INTO d{} SELECT * FROM t{} WHERE k > (SELECT COUNT(*) FROM d{}) ORDER BY k LIMIT 3", "d{}");
}

TEST_F(sql_topn_write_test, insert_select_same_table) {
    // the scan reads the table written by the statement itself
    verify_without_topn("INSERT INTO t{} SELECT k + 100, v, w FROM t{} ORDER BY k LIMIT 3", "t{}");
    verify_without_topn("INSERT INTO t{} SELECT k + 100, v, w FROM t{} ORDER BY k DESC LIMIT 3", "t{}");
    verify_without_topn("INSERT INTO t{} SELECT k + 1000, v, w FROM t{} WHERE w > 1 ORDER BY k LIMIT 5", "t{}");
}

TEST_F(sql_topn_write_test, update) {
    verify_without_topn("UPDATE t{} SET w = w + 100 WHERE k IN (SELECT k FROM t{} ORDER BY v LIMIT 3)", "t{}");
    verify_without_topn(
        "UPDATE t{} SET w = w + 100 WHERE k IN (SELECT k FROM t{} ORDER BY k DESC LIMIT 3)", "t{}");
}

TEST_F(sql_topn_write_test, delete) {
    verify_without_topn("DELETE FROM t{} WHERE k IN (SELECT k FROM t{} ORDER BY k LIMIT 3)", "t{}");
    verify_without_topn("DELETE FROM t{} WHERE k IN (SELECT k FROM t{} WHERE w > 1 ORDER BY k DESC LIMIT 3)", "t{}");
}

}  // namespace jogasaki::testing
