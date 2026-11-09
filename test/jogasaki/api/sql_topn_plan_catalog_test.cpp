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
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <tuple>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scan_impl_kind.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;
using namespace std::string_view_literals;
using namespace std::string_literals;

using kind = meta::field_type_kind;

/**
 * @brief a query in the plan catalog of TOP-N optimization
 */
struct catalog_query {
    /// @brief the id in the plan catalog
    std::string_view id_{};

    /// @brief the query
    std::string_view sql_{};

    /// @brief the indices of the output columns specified by ORDER BY, empty if the result is not ordered
    std::vector<std::size_t> order_columns_{};

    /**
     * @brief the query returning all the candidate rows if the result is not ordered but limited
     * @details the rows returned by the query must be a subset of the result of this query
     */
    std::string_view candidates_{};
};

/**
 * @brief tests that TOP-N optimizations do not change the query results
 * @details the queries come from the plan catalog provided for jogasaki in tsurugi-issues#1589, and each query
 * result is compared with the one compiled without TOP-N runtime features (F0 in the catalog).
 * The parameters are (scan_impl, use_rtx).
 * @note the sort keys of the test data are unique except for NULLs, and the number of the rows with NULL sort key
 * does not exceed the row count of LIMIT, so that the ordered results are deterministic except for the order
 * among the rows with NULL sort key.
 */
class sql_topn_plan_catalog_test :
    public ::testing::TestWithParam<std::tuple<scan_impl_kind, bool>>,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        cfg_ = std::make_shared<configuration>();
        cfg_->scan_impl(std::get<0>(GetParam()));
        // enable rtx parallel scan to verify the scans with row count or direction are not split
        cfg_->scan_default_parallel(4);
        db_setup(cfg_);
        create_tables();
    }

    void TearDown() override {
        db_teardown();
    }

    bool use_rtx() {
        return std::get<1>(GetParam());
    }

    void create_tables() {
        execute_statement("CREATE TABLE tn_t (k INT PRIMARY KEY, v INT, w INT)");
        execute_statement("CREATE INDEX tn_t_v ON tn_t (v)");
        execute_statement("CREATE TABLE tn_c (a INT, b INT, c INT, PRIMARY KEY (a, b))");
        execute_statement("CREATE TABLE tn_d (k INT, v INT, PRIMARY KEY (k DESC))");
        execute_statement("CREATE TABLE tn_n (k INT PRIMARY KEY, v INT)");

        // tn_t: 20 rows, v and w are permutations of 0..19 not aligned with k (unique except for NULL)
        // - v is NULL for k = 5, so the index tn_t_v has a NULL entry
        // - w is NULL for k = 12, and `w > 1` filters out k = 0, 7 and 12
        // - tn_t.v = 19 (k = 17) and NULL (k = 5) have no partner in tn_n
        for(std::int32_t k = 0; k < 20; ++k) {
            auto v = k == 5 ? "NULL"s : std::to_string(k * 7 % 20);
            auto w = k == 12 ? "NULL"s : std::to_string(k * 3 % 20);
            execute_statement(
                "INSERT INTO tn_t VALUES (" + std::to_string(k) + ", " + v + ", " + w + ")"
            );
        }
        // tn_c: a in 0..2, b in 0..6
        for(std::int32_t a = 0; a < 3; ++a) {
            for(std::int32_t b = 0; b < 7; ++b) {
                execute_statement(
                    "INSERT INTO tn_c VALUES (" + std::to_string(a) + ", " + std::to_string(b) + ", " +
                    std::to_string(a * 10 + b) + ")"
                );
            }
        }
        // tn_d: k in 0..9
        for(std::int32_t k = 0; k < 10; ++k) {
            execute_statement("INSERT INTO tn_d VALUES (" + std::to_string(k) + ", " + std::to_string(k * 2) + ")");
        }
        // tn_n: k in 0..18 and 50, where k = 15 and 50 have no partner in tn_t, v is unique
        for(std::int32_t k = 0; k < 19; ++k) {
            execute_statement("INSERT INTO tn_n VALUES (" + std::to_string(k) + ", " + std::to_string(1000 - k) + ")");
        }
        execute_statement("INSERT INTO tn_n VALUES (50, 950)");
    }

    std::vector<basic_record> query(std::string_view sql, bool enable_topn) {
        cfg_->enable_scan_limit(enable_topn);
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

    static std::optional<std::int64_t> field_value(basic_record const& rec, std::size_t index) {
        if(rec.is_null(index)) {
            return std::nullopt;
        }
        switch(rec.record_meta()->at(index).kind()) {
            case kind::int4: return rec.get_value<std::int32_t>(index);
            case kind::int8: return rec.get_value<std::int64_t>(index);
            default: break;
        }
        ADD_FAILURE() << "unexpected type of sort key: " << rec;
        return std::nullopt;
    }

    static std::vector<std::vector<std::optional<std::int64_t>>> sort_keys(
        std::vector<basic_record> const& records,
        std::vector<std::size_t> const& columns
    ) {
        std::vector<std::vector<std::optional<std::int64_t>>> ret{};
        ret.reserve(records.size());
        for(auto&& rec : records) {
            auto& e = ret.emplace_back();
            for(auto c : columns) {
                e.emplace_back(field_value(rec, c));
            }
        }
        return ret;
    }

    static std::vector<basic_record> sorted(std::vector<basic_record> records) {
        std::sort(records.begin(), records.end());
        return records;
    }

    static std::string to_string(std::vector<basic_record> const& records) {
        std::stringstream ss{};
        for(auto&& rec : records) {
            ss << rec << "\n";
        }
        return ss.str();
    }

    void verify(catalog_query const& q) {
        SCOPED_TRACE(std::string{q.id_} + ": " + std::string{q.sql_});
        auto expected = query(q.sql_, false);
        auto actual = query(q.sql_, true);
        if(! q.candidates_.empty()) {
            // the result is not ordered and limited, so any rows from the candidates are acceptable
            ASSERT_TRUE(q.order_columns_.empty());
            EXPECT_EQ(expected.size(), actual.size()) << to_string(actual);
            auto candidates = sorted(query(q.candidates_, false));
            auto rows = sorted(actual);
            EXPECT_TRUE(std::includes(candidates.begin(), candidates.end(), rows.begin(), rows.end()))
                << "rows:\n" << to_string(rows) << "candidates:\n" << to_string(candidates);
            return;
        }
        EXPECT_EQ(sorted(expected), sorted(actual)) << "expected:\n" << to_string(expected)
            << "actual:\n" << to_string(actual);
        if(! q.order_columns_.empty()) {
            EXPECT_EQ(sort_keys(expected, q.order_columns_), sort_keys(actual, q.order_columns_))
                << "expected:\n" << to_string(expected) << "actual:\n" << to_string(actual);
        }
    }

    void verify(std::vector<catalog_query> const& queries) {
        for(auto&& q : queries) {
            verify(q);
        }
    }

    // returns whether the plan has the scan with row count
    bool has_scan_with_count(std::string_view query, bool enable_topn) {
        cfg_->enable_scan_limit(enable_topn);
        std::string plan{};
        explain_statement(query, plan);
        auto begin = plan.find(R"("kind":"scan")");
        if(begin == std::string::npos) {
            return false;
        }
        auto end = plan.find(R"("input_ports")", begin);
        auto scan = std::string_view{plan}.substr(begin, end - begin);
        return scan.find(R"("row_slice":{"count")") != std::string_view::npos;
    }

    std::shared_ptr<configuration> cfg_{};  //NOLINT
};

INSTANTIATE_TEST_SUITE_P(
    plan_catalog,
    sql_topn_plan_catalog_test,
    ::testing::Combine(
        ::testing::Values(scan_impl_kind::automatic, scan_impl_kind::legacy, scan_impl_kind::iscan),
        ::testing::Bool()
    ),
    [](auto const& info) {
        auto impl = std::get<0>(info.param);
        return std::string{impl == scan_impl_kind::automatic ? "auto" : to_string_view(impl)} +
            (std::get<1>(info.param) ? "_rtx" : "_occ");
    }
);

TEST_P(sql_topn_plan_catalog_test, topn_features_change_plan) {
    // make sure the comparison below is not trivial
    EXPECT_FALSE(has_scan_with_count("SELECT * FROM tn_t LIMIT 3", false));
    EXPECT_TRUE(has_scan_with_count("SELECT * FROM tn_t LIMIT 3", true));
}

TEST_P(sql_topn_plan_catalog_test, a_independent_of_flags) {
    verify({
        {"A1", "SELECT k + 1 FROM tn_t LIMIT 3", {}, "SELECT k + 1 FROM tn_t"},
        {"A2", "SELECT * FROM (SELECT * FROM tn_t ORDER BY k LIMIT 10) s ORDER BY k LIMIT 3", {0}},
        {"A3", "SELECT * FROM (SELECT * FROM tn_t LIMIT 10) s LIMIT 3", {}, "SELECT * FROM tn_t"},
        {"A4", "SELECT * FROM tn_t ORDER BY k", {0}},
    });
}

TEST_P(sql_topn_plan_catalog_test, b_limit_adjacent_to_scan) {
    verify({
        {"B1", "SELECT * FROM tn_t LIMIT 3", {}, "SELECT * FROM tn_t"},
        {"B2", "SELECT * FROM tn_t LIMIT 0", {}, "SELECT * FROM tn_t"},
        {"B3", "SELECT * FROM tn_t ORDER BY k LIMIT 3", {0}},
        {"B4", "SELECT * FROM tn_t WHERE k >= 10 ORDER BY k LIMIT 3", {0}},
        {"B5", "SELECT * FROM tn_c WHERE a = 1 ORDER BY b LIMIT 3", {1}},
        {"B6", "SELECT * FROM tn_d ORDER BY k DESC LIMIT 3", {0}},
        {"B7", "SELECT * FROM tn_t ORDER BY v LIMIT 3", {1}},
        {"B8", "SELECT k + 1 FROM tn_t ORDER BY k LIMIT 3", {0}},
    });
}

TEST_P(sql_topn_plan_catalog_test, c_filter_between_scan_and_limit) {
    verify({
        {"C1", "SELECT * FROM tn_t WHERE w > 1 LIMIT 3", {}, "SELECT * FROM tn_t WHERE w > 1"},
        {"C2", "SELECT * FROM tn_t WHERE w > 1 ORDER BY k LIMIT 3", {0}},
        {"C3", "SELECT * FROM tn_t WHERE w > 1 ORDER BY v LIMIT 3", {1}},
    });
}

TEST_P(sql_topn_plan_catalog_test, d_not_optimized) {
    verify({
        {"D1", "SELECT * FROM tn_t ORDER BY k DESC LIMIT 3", {0}},
        {"D2", "SELECT * FROM tn_t ORDER BY w LIMIT 3", {2}},
        {"D3", "SELECT * FROM tn_t WHERE v = 5 ORDER BY k LIMIT 3", {0}},
        {"D4", "SELECT * FROM tn_t t JOIN tn_n n ON t.k = n.k ORDER BY t.k LIMIT 3", {0}},
        {"D5", "SELECT v, COUNT(*) FROM tn_t GROUP BY v ORDER BY v LIMIT 3", {0}},
    });
}

TEST_P(sql_topn_plan_catalog_test, e_backward_scan) {
    verify({
        {"E1", "SELECT * FROM tn_t WHERE k < 4 ORDER BY k DESC LIMIT 3", {0}},
        {"E2", "SELECT * FROM tn_d ORDER BY k LIMIT 3", {0}},
        {"E3", "SELECT * FROM tn_t ORDER BY v DESC LIMIT 3", {1}},
        {"E4", "SELECT * FROM tn_c WHERE a = 1 ORDER BY b DESC LIMIT 3", {1}},
        {"E5", "SELECT * FROM tn_c ORDER BY a, b DESC LIMIT 3", {0, 1}},
        {"E6", "SELECT * FROM tn_t WHERE w > 1 ORDER BY k DESC LIMIT 3", {0}},
    });
}

TEST_P(sql_topn_plan_catalog_test, o_order_by_without_limit) {
    verify({
        {"O1", "SELECT * FROM tn_t ORDER BY k DESC", {0}},
        {"O2", "SELECT * FROM tn_t ORDER BY v", {1}},
        {"O3", "SELECT * FROM tn_t WHERE w > 1 ORDER BY k", {0}},
        {"O4", "SELECT * FROM tn_t ORDER BY w", {2}},
    });
}

TEST_P(sql_topn_plan_catalog_test, g_across_join) {
    verify({
        {"G1", "SELECT * FROM tn_t t JOIN tn_n n ON t.v = n.k ORDER BY t.k LIMIT 3", {0}},
        {"G2", "SELECT * FROM tn_t t JOIN tn_n n ON t.v = n.k ORDER BY t.k", {0}},
        {"G3", "SELECT * FROM tn_t t JOIN tn_n n ON t.v = n.k ORDER BY t.k DESC LIMIT 3", {0}},
        {"G4", "SELECT * FROM tn_t t JOIN tn_n n ON t.v = n.k ORDER BY n.v LIMIT 3", {4}},
        {"G5", "SELECT * FROM tn_t t LEFT OUTER JOIN tn_n n ON t.v = n.k ORDER BY t.k LIMIT 3", {0}},
        {"G6", "SELECT * FROM tn_t t LEFT OUTER JOIN tn_n n ON t.v = n.k LIMIT 3", {},
            "SELECT * FROM tn_t t LEFT OUTER JOIN tn_n n ON t.v = n.k"},
        {"G7", "SELECT * FROM tn_t t LEFT OUTER JOIN tn_n n ON t.v = n.k ORDER BY n.v LIMIT 3", {4}},
        {"G8", "SELECT * FROM tn_t t LEFT OUTER JOIN tn_n n ON t.v = n.k ORDER BY t.k LIMIT 2000", {0}},
        {"G9", "SELECT * FROM tn_t t WHERE t.v IN (SELECT n.k FROM tn_n n) ORDER BY t.k LIMIT 3", {0}},
        {"G10", "SELECT * FROM tn_t t WHERE NOT EXISTS (SELECT * FROM tn_n n WHERE n.k = t.v) ORDER BY t.k LIMIT 3",
            {0}},
        {"G11", "SELECT * FROM tn_t t FULL OUTER JOIN tn_n n ON t.v = n.k ORDER BY t.k LIMIT 3", {0}},
    });
}

TEST_P(sql_topn_plan_catalog_test, h_scalar_subquery) {
    verify({
        {"H1", "SELECT t.k, (SELECT n.v FROM tn_n n WHERE n.k = t.v) FROM tn_t t ORDER BY t.k LIMIT 3", {0}},
        {"H2", "SELECT t.k, (SELECT n.v FROM tn_n n WHERE n.k = t.v) FROM tn_t t LIMIT 3", {},
            "SELECT t.k, (SELECT n.v FROM tn_n n WHERE n.k = t.v) FROM tn_t t"},
    });
}

TEST_P(sql_topn_plan_catalog_test, i_index_selection) {
    verify({
        {"I1", "SELECT * FROM tn_t WHERE v > 5 ORDER BY k LIMIT 3", {0}},
        {"I2", "SELECT * FROM tn_t WHERE v > 5 ORDER BY v LIMIT 3", {1}},
        {"I3", "SELECT * FROM tn_t WHERE v = 5 ORDER BY v LIMIT 3", {1}},
    });
}

}  // namespace jogasaki::testing
