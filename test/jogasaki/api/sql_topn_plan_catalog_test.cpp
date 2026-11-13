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

#include <takatori/type/primitive.h>
#include <takatori/type/table.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>

#include <jogasaki/configuration.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scan_impl_kind.h>
#include <jogasaki/test_utils/mock_any_sequence_stream.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;
using namespace std::string_view_literals;
using namespace std::string_literals;

using kind = meta::field_type_kind;

namespace {

constexpr std::size_t tvf_id_tn_f = 12100;

/**
 * @brief mock table-valued function `tn_f(x INT) -> TABLE(c1 INT)` for APPLY
 * @details returns 2 rows (x * 10, x * 10 + 1) if x % 3 = 0, no rows if x % 3 = 1, and 1 row (x * 10) if x % 3 = 2.
 */
std::unique_ptr<data::any_sequence_stream> tn_f(
    executor::expr::evaluator_context& /* ctx */,
    takatori::util::sequence_view<data::any> args
) {
    auto x = args[0].to<std::int32_t>();
    mock_any_sequence_stream::sequences_type sequences{};
    switch(x % 3) {
        case 0:
            sequences.emplace_back(data::any_sequence::storage_type{
                data::any{std::in_place_type<std::int32_t>, x * 10}
            });
            sequences.emplace_back(data::any_sequence::storage_type{
                data::any{std::in_place_type<std::int32_t>, x * 10 + 1}
            });
            break;
        case 2:
            sequences.emplace_back(data::any_sequence::storage_type{
                data::any{std::in_place_type<std::int32_t>, x * 10}
            });
            break;
        default: break;
    }
    return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
}

}  // namespace

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
 * @details the queries come from the plan catalog provided for jogasaki in tsurugi-issues#1589 and the plan
 * changes notified after that in the same issue, and each query result is compared with the one compiled without
 * TOP-N runtime features (F0 in the catalog).
 * The parameters are (scan_impl, use_rtx, flags), where flags is the TOP-N runtime features to compare with F0:
 * F1 (scan_limit), F2 (ordered_scan and ordered_forward), F3 (F1 and F2) or F4 (F3 and backward_scan).
 * @note the sort keys of the test data are unique except for NULLs, and the number of the rows with NULL sort key
 * does not exceed the row count of LIMIT, so that the ordered results are deterministic except for the order
 * among the rows with NULL sort key. The exception is APPLY, whose ties are noted in the test.
 */
enum class topn_flags {
    f1,
    f2,
    f3,
    f4,
};

class sql_topn_plan_catalog_test :
    public ::testing::TestWithParam<std::tuple<scan_impl_kind, bool, topn_flags>>,
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
        register_functions();
    }

    void TearDown() override {
        unregister_functions();
        db_teardown();
    }

    void register_functions() {
        namespace t = takatori::type;
        decl_tn_f_ = global::regular_function_provider()->add(std::make_shared<yugawara::function::declaration>(
            tvf_id_tn_f,
            "tn_f",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int4>()},
            }),
            std::vector<std::shared_ptr<t::data const>>{
                std::make_shared<t::int4>(),
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        ));
        global::table_valued_function_repository().add(
            tvf_id_tn_f,
            std::make_shared<executor::function::table_valued_function_info>(
                executor::function::table_valued_function_kind::builtin,
                tn_f,
                1,
                executor::function::table_valued_function_info::columns_type{
                    executor::function::table_valued_function_column{"c1"},
                }
            )
        );
    }

    void unregister_functions() {
        global::table_valued_function_repository().clear();
        if(decl_tn_f_) {
            global::regular_function_provider()->remove(*decl_tn_f_);
            decl_tn_f_.reset();
        }
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

    // enables the TOP-N runtime features of the parameter if enable_topn is true, or disables all of them (F0)
    void set_topn_flags(bool enable_topn) {
        auto flags = std::get<2>(GetParam());
        cfg_->enable_scan_limit(enable_topn && flags != topn_flags::f2);
        cfg_->enable_ordered_scan(enable_topn && flags != topn_flags::f1);
        cfg_->enable_ordered_forward(true);
        cfg_->enable_backward_scan(enable_topn && flags == topn_flags::f4);
    }

    /**
     * @brief creates the table with more rows than the default `max_partial_limit_count` (1000)
     * @details tn_b has k in 0..1599 and v as a permutation of k (unique), where v in 0..18 except 15 has a partner
     * in tn_n.
     */
    void create_large_table() {
        execute_statement("CREATE TABLE tn_b (k INT PRIMARY KEY, v INT)");
        execute_statement("CREATE INDEX tn_b_v ON tn_b (v)");
        // read tn_t to fill tn_b, as reading tn_b itself can hit Halloween problem with dev_scan_impl=iscan
        execute_statement(
            "INSERT INTO tn_b "
            "SELECT x.k * 80 + y.k * 4 + z.k, (x.k * 80 + y.k * 4 + z.k) * 7919 % 1600 "
            "FROM tn_t x CROSS JOIN tn_t y CROSS JOIN tn_t z WHERE z.k < 4"
        );
    }

    std::vector<basic_record> query(std::string_view sql, bool enable_topn) {
        set_topn_flags(enable_topn);
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

    // returns the plan of the query in json
    std::string plan(std::string_view query, bool enable_topn) {
        set_topn_flags(enable_topn);
        std::string ret{};
        explain_statement(query, ret);
        return ret;
    }

    std::shared_ptr<configuration> cfg_{};  //NOLINT
    std::shared_ptr<yugawara::function::declaration> decl_tn_f_{};  //NOLINT
};

INSTANTIATE_TEST_SUITE_P(
    plan_catalog,
    sql_topn_plan_catalog_test,
    // F1 is compared with each scan implementation, while F2, F3 and F4 are compared only with the automatic one
    ::testing::Values(
        std::make_tuple(scan_impl_kind::automatic, false, topn_flags::f1),
        std::make_tuple(scan_impl_kind::automatic, true, topn_flags::f1),
        std::make_tuple(scan_impl_kind::legacy, false, topn_flags::f1),
        std::make_tuple(scan_impl_kind::legacy, true, topn_flags::f1),
        std::make_tuple(scan_impl_kind::iscan, false, topn_flags::f1),
        std::make_tuple(scan_impl_kind::iscan, true, topn_flags::f1),
        std::make_tuple(scan_impl_kind::automatic, false, topn_flags::f2),
        std::make_tuple(scan_impl_kind::automatic, true, topn_flags::f2),
        std::make_tuple(scan_impl_kind::automatic, false, topn_flags::f3),
        std::make_tuple(scan_impl_kind::automatic, true, topn_flags::f3),
        std::make_tuple(scan_impl_kind::automatic, false, topn_flags::f4),
        std::make_tuple(scan_impl_kind::automatic, true, topn_flags::f4)
    ),
    [](auto const& info) {
        auto impl = std::get<0>(info.param);
        auto flags = std::get<2>(info.param);
        return std::string{flags == topn_flags::f1 ? "f1_" : flags == topn_flags::f2 ? "f2_" : flags == topn_flags::f3 ? "f3_" : "f4_"} +
            std::string{impl == scan_impl_kind::automatic ? "auto" : to_string_view(impl)} +
            (std::get<1>(info.param) ? "_rtx" : "_occ");
    }
);

TEST_P(sql_topn_plan_catalog_test, topn_features_change_plan) {
    // make sure the comparison below is not trivial
    EXPECT_NE(plan("SELECT * FROM tn_t ORDER BY k LIMIT 3", false), plan("SELECT * FROM tn_t ORDER BY k LIMIT 3", true));
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

/**
 * @brief tests for the plan changes notified after the plan catalog
 * @details same as sql_topn_plan_catalog_test, but runs only with the automatic scan implementation to save time
 */
class sql_topn_plan_notice_test : public sql_topn_plan_catalog_test {};

INSTANTIATE_TEST_SUITE_P(
    plan_notice,
    sql_topn_plan_notice_test,
    ::testing::Values(
        std::make_tuple(scan_impl_kind::automatic, false, topn_flags::f1),
        std::make_tuple(scan_impl_kind::automatic, true, topn_flags::f1),
        std::make_tuple(scan_impl_kind::automatic, false, topn_flags::f2),
        std::make_tuple(scan_impl_kind::automatic, true, topn_flags::f2),
        std::make_tuple(scan_impl_kind::automatic, false, topn_flags::f3),
        std::make_tuple(scan_impl_kind::automatic, true, topn_flags::f3),
        std::make_tuple(scan_impl_kind::automatic, false, topn_flags::f4),
        std::make_tuple(scan_impl_kind::automatic, true, topn_flags::f4)
    ),
    [](auto const& info) {
        auto flags = std::get<2>(info.param);
        return std::string{flags == topn_flags::f1 ? "f1" : flags == topn_flags::f2 ? "f2" : flags == topn_flags::f3 ? "f3" : "f4"} +
            (std::get<1>(info.param) ? "_rtx" : "_occ");
    }
);

// the plan changes notified after the catalog: removal of TRUE filters and partial limits before OUTER APPLY
// (linking limits with ordered forward changes only G5, which is covered above)

TEST_P(sql_topn_plan_notice_test, true_filter_removed) {
    verify({
        {"TF-a", "SELECT * FROM (SELECT k, v, w + 1 AS x FROM tn_t) s WHERE v > 1 ORDER BY v LIMIT 3", {1}},
        {"TF-b", "SELECT * FROM tn_t t LEFT OUTER JOIN tn_n n ON t.v = n.k WHERE t.w > 1 ORDER BY t.k LIMIT 3",
            {0}},
    });
}

TEST_P(sql_topn_plan_notice_test, partial_limit_before_outer_apply) {
    // tn_f(k) returns 2, 0 and 1 rows for k % 3 = 0, 1 and 2 respectively, and OUTER APPLY gives a row with NULL
    // if it returns no rows, so the rows for k = 0, 1, 2, 3 are 2, 1, 1, 2 rows (and for k = 1, 2, 3, 4 with w > 1).
    // The LIMITs below are chosen so that the ties of t.k are not split.
    verify({
        {"AP-a", "SELECT t.k, r.c1 FROM tn_t t OUTER APPLY tn_f(t.k) AS r(c1) ORDER BY t.k LIMIT 4", {0}},
        {"AP-b", "SELECT t.k, r.c1 FROM tn_t t OUTER APPLY tn_f(t.k) AS r(c1) WHERE t.w > 1 ORDER BY t.k LIMIT 4",
            {0}},
        {"AP-c", "SELECT t.k, r.c1 FROM tn_t t OUTER APPLY tn_f(t.k) AS r(c1) LIMIT 3", {},
            "SELECT t.k, r.c1 FROM tn_t t OUTER APPLY tn_f(t.k) AS r(c1)"},
        // not optimized - the sort key uses the output of the function, or CROSS APPLY can drop rows
        {"AP-d", "SELECT t.k, r.c1 FROM tn_t t OUTER APPLY tn_f(t.k) AS r(c1) ORDER BY t.k, r.c1 LIMIT 3",
            {0, 1}},
        {"AP-e", "SELECT t.k, r.c1 FROM tn_t t CROSS APPLY tn_f(t.k) AS r(c1) ORDER BY t.k LIMIT 3", {0}},
    });
}

// the plan changes notified after the catalog: large LIMIT, nested limits and limits on UNION ALL branches

TEST_P(sql_topn_plan_notice_test, large_limit) {
    create_large_table();
    // the row count around max_early_stop_limit_count (10000)
    verify({
        {"LL-a", "SELECT * FROM tn_t WHERE v > 5 ORDER BY k LIMIT 100000000", {0}},
        {"LL-b", "SELECT * FROM tn_b WHERE v < 100 ORDER BY k LIMIT 10000", {0}},
        {"LL-c", "SELECT * FROM tn_b WHERE v < 100 ORDER BY k LIMIT 10001", {0}},
        {"LL-d", "SELECT * FROM tn_b LIMIT 100000000", {}, "SELECT * FROM tn_b"},
    });
}

TEST_P(sql_topn_plan_notice_test, nested_limits) {
    create_large_table();
    verify({
        // the outer row count exceeds max_partial_limit_count (1000)
        {"NL-a",
            "SELECT * FROM (SELECT * FROM tn_b ORDER BY k LIMIT 1500) s LEFT OUTER JOIN tn_n n ON s.v = n.k "
            "ORDER BY s.k LIMIT 1200", {0}},
        // the inner sort key is a prefix of the outer one, where the inner ties of `a` are not split by the outer
        {"NL-b", "SELECT * FROM (SELECT * FROM tn_c ORDER BY a LIMIT 10) s ORDER BY a, b LIMIT 3", {0, 1}},
        {"NL-c", "SELECT * FROM tn_b b LEFT OUTER JOIN tn_n n ON b.v = n.k LIMIT 1200", {},
            "SELECT * FROM tn_b b LEFT OUTER JOIN tn_n n ON b.v = n.k"},
        {"NL-d", "SELECT b.k, r.c1 FROM tn_b b OUTER APPLY tn_f(b.k) AS r(c1) LIMIT 1200", {},
            "SELECT b.k, r.c1 FROM tn_b b OUTER APPLY tn_f(b.k) AS r(c1)"},
    });
}

TEST_P(sql_topn_plan_notice_test, limit_over_union_all) {
    create_large_table();
    verify({
        {"UA-a",
            "SELECT * FROM (SELECT k, v FROM tn_t UNION ALL SELECT k + 100, v FROM tn_n) x ORDER BY k LIMIT 3", {0}},
        {"UA-b",
            "SELECT * FROM (SELECT k, v FROM tn_t UNION ALL SELECT k + 100, v FROM tn_n) x ORDER BY k DESC LIMIT 3",
            {0}},
        {"UA-c",
            "SELECT * FROM (SELECT k, v FROM tn_t UNION ALL SELECT k, v FROM tn_n) x ORDER BY k, v LIMIT 5", {0, 1}},
        {"UA-d",
            "SELECT * FROM (SELECT k, v FROM tn_t WHERE w > 1 UNION ALL SELECT k + 100, v FROM tn_n) x "
            "ORDER BY v LIMIT 3", {1}},
        {"UA-e",
            "SELECT * FROM (SELECT k, v FROM tn_t UNION ALL SELECT k + 100, v FROM tn_n UNION ALL "
            "SELECT k + 200, v FROM tn_d) x ORDER BY k DESC LIMIT 4", {0}},
        {"UA-f", "SELECT * FROM (SELECT k, v FROM tn_t UNION ALL SELECT k, v FROM tn_n) x LIMIT 3", {},
            "SELECT * FROM (SELECT k, v FROM tn_t UNION ALL SELECT k, v FROM tn_n) x"},
        {"UA-g",
            "SELECT * FROM (SELECT k, v FROM tn_t WHERE w > 1 UNION ALL SELECT k, v FROM tn_n WHERE v < 990) x LIMIT 3",
            {}, "SELECT * FROM (SELECT k, v FROM tn_t WHERE w > 1 UNION ALL SELECT k, v FROM tn_n WHERE v < 990) x"},
        // the row count exceeds max_partial_limit_count (1000), so only the scans have the count
        {"UA-h", "SELECT * FROM (SELECT k, v FROM tn_b UNION ALL SELECT k, v FROM tn_t) x LIMIT 1200", {},
            "SELECT * FROM (SELECT k, v FROM tn_b UNION ALL SELECT k, v FROM tn_t) x"},
        {"UA-i",
            "SELECT * FROM (SELECT k, v FROM tn_b UNION ALL SELECT k + 10000, v FROM tn_t) x ORDER BY k LIMIT 1200",
            {0}},
        // not optimized - UNION removes duplicates
        {"UA-j", "SELECT * FROM (SELECT k FROM tn_t UNION SELECT k FROM tn_n) x ORDER BY k LIMIT 3", {0}},
    });
}

}  // namespace jogasaki::testing
