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
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::mock;
using namespace std::string_view_literals;

enum class tx_kind {
    occ,
    ltx,
    rtx,
};

/**
 * @brief tests for the backward scan (tsurugi-issues#1589 R7)
 * @details each ordered query result is compared with the one compiled without backward_scan (other TOP-N features
 * are enabled). The sort keys are unique (with the primary key), so the results are deterministic.
 * The parameter is the transaction kind.
 */
class sql_backward_scan_test :
    public ::testing::TestWithParam<tx_kind>,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        cfg_ = std::make_shared<configuration>();
        // enable rtx parallel scan to verify the backward scan is not split
        cfg_->scan_default_parallel(4);
        // yield frequently to verify the backward scan resumes correctly
        cfg_->scan_block_size(5);
        cfg_->scan_yield_interval(0);
        db_setup(cfg_);
        create_tables();
    }

    void TearDown() override {
        if(gc_block_) {
            EXPECT_EQ(status::ok, gc_block_->abort());
        }
        db_teardown();
    }

    // t: k in 0..59 except the deleted ones, v = k * 7 % 60 (unique) with NULL for k % 10 == 3, w = k % 7,
    //    dev = k % 4 with the index t_dev
    // d: same as t, but the primary key is descending
    // c: the composite primary key (a, b)
    // m: k in 0..179 joined with t by f = k / 3
    void create_tables() {
        execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v INT, w INT, dev INT)");
        execute_statement("CREATE INDEX t_v ON t (v)");
        execute_statement("CREATE INDEX t_dev ON t (dev)");
        execute_statement("CREATE TABLE d (k INT, v INT, PRIMARY KEY (k DESC))");
        execute_statement("CREATE TABLE c (a INT, b INT, x INT, PRIMARY KEY (a, b))");
        execute_statement("CREATE TABLE m (k INT PRIMARY KEY, f INT)");
        std::string t_values{};
        std::string d_values{};
        for(std::int32_t i = 0; i < 60; ++i) {
            auto k = (i * 37) % 60;
            auto v = k % 10 == 3 ? std::string{"NULL"} : std::to_string(k * 7 % 60);
            t_values += (i == 0 ? "" : ", ");
            t_values += "(" + std::to_string(k) + ", " + v + ", " + std::to_string(k % 7) + ", " +
                std::to_string(k % 4) + ")";
            d_values += (i == 0 ? "" : ", ");
            d_values += "(" + std::to_string(k) + ", " + std::to_string(k * 2) + ")";
        }
        execute_statement("INSERT INTO t VALUES " + t_values);
        execute_statement("INSERT INTO d VALUES " + d_values);
        std::string c_values{};
        for(std::int32_t a = 0; a < 4; ++a) {
            for(std::int32_t b = 0; b < 15; ++b) {
                c_values += (c_values.empty() ? "" : ", ");
                c_values += "(" + std::to_string(a) + ", " + std::to_string(b) + ", " + std::to_string(a * 100 + b) + ")";
            }
        }
        execute_statement("INSERT INTO c VALUES " + c_values);
        std::string m_values{};
        for(std::int32_t k = 0; k < 180; ++k) {
            m_values += (k == 0 ? "" : ", ");
            m_values += "(" + std::to_string(k) + ", " + std::to_string(k / 3) + ")";
        }
        execute_statement("INSERT INTO m VALUES " + m_values);

        // keep the long transaction open to prevent tombstones from being garbage-collected, so that the backward
        // scan skips them
        gc_block_ = utils::create_transaction(*db_, false, true);
        execute_statement("DELETE FROM t WHERE k = 59 OR k = 57 OR k = 30 OR k = 1");
        execute_statement("DELETE FROM d WHERE k = 0 OR k = 2 OR k = 30");
    }

    std::vector<basic_record> query(std::string_view sql, bool backward) {
        cfg_->enable_backward_scan(backward);
        std::vector<basic_record> result{};
        switch(GetParam()) {
            case tx_kind::occ: execute_query(sql, result); break;
            case tx_kind::ltx: {
                auto tx = utils::create_transaction(*db_, false, true);
                execute_query(sql, *tx, result);
                EXPECT_EQ(status::ok, tx->commit());
                break;
            }
            case tx_kind::rtx: {
                auto tx = utils::create_transaction(*db_, true, false);
                execute_query(sql, *tx, result);
                EXPECT_EQ(status::ok, tx->commit());
                break;
            }
        }
        return result;
    }

    bool has_backward_scan(std::string_view sql) {
        cfg_->enable_backward_scan(true);
        std::string plan{};
        explain_statement(sql, plan);
        return plan.find(R"("direction":"backward")") != std::string::npos;
    }

    static std::string to_string(std::vector<basic_record> const& records) {
        std::stringstream ss{};
        for(auto&& rec : records) {
            ss << rec << "\n";
        }
        return ss.str();
    }

    // verifies the query has the same ordered result with and without backward_scan
    // (expect_backward verifies the plan has the backward scan, but the plan can change)
    void verify(std::string_view sql, bool expect_backward = true) {
        SCOPED_TRACE(sql);
        if(expect_backward) {
            EXPECT_TRUE(has_backward_scan(sql));
        }
        auto expected = query(sql, false);
        auto actual = query(sql, true);
        EXPECT_FALSE(expected.empty());
        EXPECT_EQ(expected, actual) << "expected:\n" << to_string(expected) << "actual:\n" << to_string(actual);
    }

    std::shared_ptr<configuration> cfg_{};  //NOLINT
    std::shared_ptr<api::transaction_handle> gc_block_{};  //NOLINT
};

INSTANTIATE_TEST_SUITE_P(
    backward,
    sql_backward_scan_test,
    ::testing::Values(tx_kind::occ, tx_kind::ltx, tx_kind::rtx),
    [](auto const& info) {
        switch(info.param) {
            case tx_kind::occ: return std::string{"occ"};
            case tx_kind::ltx: return std::string{"ltx"};
            case tx_kind::rtx: return std::string{"rtx"};
        }
        return std::string{};
    }
);

TEST_P(sql_backward_scan_test, primary_key) {
    verify("SELECT * FROM t ORDER BY k DESC LIMIT 3");
    verify("SELECT * FROM t ORDER BY k DESC LIMIT 100");
    verify("SELECT * FROM t ORDER BY k DESC");
}

TEST_P(sql_backward_scan_test, range) {
    verify("SELECT * FROM t WHERE k < 31 ORDER BY k DESC LIMIT 3");
    verify("SELECT * FROM t WHERE k <= 30 ORDER BY k DESC LIMIT 3");
    verify("SELECT * FROM t WHERE k > 10 AND k <= 40 ORDER BY k DESC");
    verify("SELECT * FROM t WHERE k >= 10 AND k < 40 ORDER BY k DESC LIMIT 5");
}

TEST_P(sql_backward_scan_test, filter) {
    // the scan has no row count because of the filter, and stops by the ordered forward exchange
    verify("SELECT * FROM t WHERE w > 1 ORDER BY k DESC LIMIT 3");
    verify("SELECT * FROM t WHERE w > 1 ORDER BY k DESC LIMIT 30");
}

TEST_P(sql_backward_scan_test, secondary_index) {
    verify("SELECT * FROM t ORDER BY v DESC LIMIT 5");
    verify("SELECT * FROM t WHERE v < 30 ORDER BY v DESC LIMIT 5");
    // without LIMIT, the compiler does not choose t_v but sorts the primary index, because reading the whole
    // secondary index with the primary index lookups is estimated more costly than the sort (O2 in the plan catalog)
    verify("SELECT * FROM t ORDER BY v DESC", false);
}

TEST_P(sql_backward_scan_test, ties_ordered_by_primary_key) {
    // the entries with the same secondary key are read in the reverse order of the primary key
    verify("SELECT * FROM t WHERE dev = 2 ORDER BY k DESC LIMIT 5");
    verify("SELECT * FROM t ORDER BY dev DESC, k DESC LIMIT 20");
}

TEST_P(sql_backward_scan_test, descending_primary_key) {
    // ascending order on the descending primary key
    verify("SELECT * FROM d ORDER BY k LIMIT 3");
    verify("SELECT * FROM d WHERE k > 20 ORDER BY k LIMIT 3");
    verify("SELECT * FROM d ORDER BY k");
}

TEST_P(sql_backward_scan_test, composite_primary_key) {
    verify("SELECT * FROM c WHERE a = 1 ORDER BY b DESC LIMIT 3");
    verify("SELECT * FROM c ORDER BY a DESC, b DESC LIMIT 20");
}

TEST_P(sql_backward_scan_test, join) {
    // the compiler scans m and finds t by its primary key (even if m.f has an index), so the sort key t.k is on the
    // right side of the join and the sort remains - this is the result check of the join with descending order
    verify("SELECT t.k, m.k FROM t JOIN m ON t.k = m.f ORDER BY t.k DESC LIMIT 10", false);
    // the backward scan of the left side of the join
    verify("SELECT t.k, t.v FROM t LEFT OUTER JOIN d ON t.k = d.k ORDER BY t.k DESC LIMIT 10");
}

}  // namespace jogasaki::testing
