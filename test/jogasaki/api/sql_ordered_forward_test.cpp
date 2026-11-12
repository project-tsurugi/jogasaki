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

/**
 * @brief tests for the ordered scan and the ordered forward exchange (tsurugi-issues#1589 R4)
 * @details each query result is compared with the one compiled without ordered_scan and ordered_forward.
 * The sort keys are unique, so the ordered results are deterministic. The parameter is use_rtx.
 */
class sql_ordered_forward_test :
    public ::testing::TestWithParam<bool>,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        cfg_ = std::make_shared<configuration>();
        // enable rtx parallel scan to verify the ordered scan is not split
        cfg_->scan_default_parallel(4);
        // yield frequently to verify the order is kept across yields
        cfg_->scan_block_size(10);
        cfg_->scan_yield_interval(0);
        db_setup(cfg_);
        create_tables();
    }

    void TearDown() override {
        db_teardown();
    }

    // t: k in 0..199, v = k * 7 % 200 (unique), w = k % 7
    // u: k in 0..199 step 3, v = k % 10
    // m: k in 0..599, f = k / 3 (each value of f appears 3 times, joined with t.k)
    void create_tables() {
        execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v INT, w INT)");
        execute_statement("CREATE INDEX t_v ON t (v)");
        execute_statement("CREATE TABLE u (k INT PRIMARY KEY, v INT)");
        execute_statement("CREATE TABLE m (k INT PRIMARY KEY, f INT)");
        execute_statement("CREATE INDEX m_f ON m (f)");
        std::string t_values{};
        for(std::int32_t k = 0; k < 200; ++k) {
            t_values += (k == 0 ? "" : ", ");
            t_values += "(" + std::to_string(k) + ", " + std::to_string(k * 7 % 200) + ", " + std::to_string(k % 7) + ")";
        }
        execute_statement("INSERT INTO t VALUES " + t_values);
        std::string u_values{};
        for(std::int32_t k = 0; k < 200; k += 3) {
            u_values += (k == 0 ? "" : ", ");
            u_values += "(" + std::to_string(k) + ", " + std::to_string(k % 10) + ")";
        }
        execute_statement("INSERT INTO u VALUES " + u_values);
        std::string m_values{};
        for(std::int32_t k = 0; k < 600; ++k) {
            m_values += (k == 0 ? "" : ", ");
            m_values += "(" + std::to_string(k) + ", " + std::to_string(k / 3) + ")";
        }
        execute_statement("INSERT INTO m VALUES " + m_values);
    }

    std::vector<basic_record> query(std::string_view sql, bool ordered) {
        cfg_->enable_ordered_scan(ordered);
        std::vector<basic_record> result{};
        if(GetParam()) {
            auto tx = utils::create_transaction(*db_, true, false);
            execute_query(sql, *tx, result);
            EXPECT_EQ(status::ok, tx->commit());
        } else {
            execute_query(sql, result);
        }
        return result;
    }

    bool has_ordered_forward(std::string_view sql) {
        cfg_->enable_ordered_scan(true);
        std::string plan{};
        explain_statement(sql, plan);
        return plan.find(R"("respect_input")") != std::string::npos;
    }

    static std::string to_string(std::vector<basic_record> const& records) {
        std::stringstream ss{};
        for(auto&& rec : records) {
            ss << rec << "\n";
        }
        return ss.str();
    }

    // verifies the query has the same ordered result with and without ordered_scan and ordered_forward
    // (expect_ordered_forward verifies the plan has the ordered forward exchange, but the plan can change)
    void verify(std::string_view sql, bool expect_ordered_forward = true) {
        SCOPED_TRACE(sql);
        if(expect_ordered_forward) {
            EXPECT_TRUE(has_ordered_forward(sql));
        }
        auto expected = query(sql, false);
        auto actual = query(sql, true);
        EXPECT_EQ(expected, actual) << "expected:\n" << to_string(expected) << "actual:\n" << to_string(actual);
    }

    std::shared_ptr<configuration> cfg_{};  //NOLINT
};

INSTANTIATE_TEST_SUITE_P(
    ordered_forward,
    sql_ordered_forward_test,
    ::testing::Bool(),
    [](auto const& info) {
        return std::string{info.param ? "rtx" : "occ"};
    }
);

TEST_P(sql_ordered_forward_test, filter) {
    // the scan has no row count because of the filter, so the ordered forward takes the first rows
    verify("SELECT * FROM t WHERE w > 1 ORDER BY k LIMIT 3");
    verify("SELECT * FROM t WHERE w > 1 ORDER BY k LIMIT 100");
    // the filter prevents the row count on the scan of t_v, so the compiler sorts the primary index instead of
    // reading t_v (C3 in the plan catalog)
    verify("SELECT * FROM t WHERE w > 1 ORDER BY v LIMIT 30", false);
    verify("SELECT * FROM t WHERE w > 1 ORDER BY k LIMIT 1000");
}

TEST_P(sql_ordered_forward_test, inner_join) {
    // each row of t joins with 3 rows of m, but the compiler scans m and finds t by its primary key instead of
    // reading m_f for each row of t, so the sort key t.k is on the right side of the join and the sort remains
    verify("SELECT t.k, m.k FROM t JOIN m ON t.k = m.f ORDER BY t.k LIMIT 3", false);
    verify("SELECT t.k, t.v, u.v FROM t JOIN u ON t.k = u.k ORDER BY t.k LIMIT 10");
    verify("SELECT t.k, t.v, u.v FROM t JOIN u ON t.k = u.k WHERE t.w > 1 ORDER BY t.k LIMIT 10");
}

TEST_P(sql_ordered_forward_test, left_outer_join) {
    verify("SELECT t.k, u.v FROM t LEFT OUTER JOIN u ON t.k = u.k ORDER BY t.k LIMIT 10");
    verify("SELECT t.k, u.v FROM t LEFT OUTER JOIN u ON t.k = u.k WHERE t.w > 1 ORDER BY t.k LIMIT 10");
}

TEST_P(sql_ordered_forward_test, semi_and_anti_join) {
    // the compiler makes the semi and anti joins of these with join_group (cogroup), which does not keep the order
    // (unlike the semi join with join_find for IN, G9 in the plan catalog), so the sort remains
    verify("SELECT * FROM t WHERE EXISTS (SELECT * FROM u WHERE u.k = t.k) ORDER BY t.k LIMIT 10", false);
    verify("SELECT * FROM t WHERE NOT EXISTS (SELECT * FROM u WHERE u.k = t.k) ORDER BY t.k LIMIT 10", false);
}

TEST_P(sql_ordered_forward_test, order_by_without_limit) {
    // the ordered scan and emit are in the same process without exchange, so no ordered forward is needed
    // (O3 in the plan catalog)
    verify("SELECT * FROM t WHERE w > 1 ORDER BY k", false);
    // without LIMIT, the compiler does not choose t_v but sorts the primary index, because reading the whole
    // secondary index with the primary index lookups is estimated more costly than the sort (O2 in the plan catalog)
    verify("SELECT * FROM t ORDER BY v", false);
}

}  // namespace jogasaki::testing
