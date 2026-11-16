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
#include <jogasaki/test_utils/custom_any_sequence_stream.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::mock;
using namespace std::string_view_literals;

namespace {

constexpr std::size_t tvf_id_tn_a = 12500;

/**
 * @brief mock table-valued function `tn_a(x INT) -> TABLE(c1 INT)`
 * @details returns 2 rows (x * 10, x * 10 + 1) if x % 3 = 0, no rows if x % 3 = 1, and 1 row (x * 10) if x % 3 = 2.
 * It returns not_ready once before the rows if x is even, so that the apply operator yields.
 */
std::unique_ptr<data::any_sequence_stream> tn_a(
    executor::expr::evaluator_context& /* ctx */,
    takatori::util::sequence_view<data::any> args
) {
    auto x = args[0].to<std::int32_t>();
    std::vector<std::int32_t> rows{};
    switch(x % 3) {
        case 0: rows = {x * 10, x * 10 + 1}; break;
        case 2: rows = {x * 10}; break;
        default: break;
    }
    return std::make_unique<custom_any_sequence_stream>(
        [rows = std::move(rows), yields = (x % 2 == 0), n = std::size_t{0}](
            data::any_sequence& seq
        ) mutable -> data::any_sequence_stream::status_type {
            if(yields) {
                yields = false;
                return data::any_sequence_stream::status_type::not_ready;
            }
            if(n < rows.size()) {
                seq = data::any_sequence{data::any_sequence::storage_type{
                    data::any{std::in_place_type<std::int32_t>, rows[n]}
                }};
                ++n;
                return data::any_sequence_stream::status_type::ok;
            }
            return data::any_sequence_stream::status_type::end_of_stream;
        }
    );
}

}  // namespace

/**
 * @brief tests for runtime_feature::apply_preserves_order (tsurugi-issues#1589)
 * @details each ordered query result is compared with the one compiled without apply_preserves_order (other TOP-N
 * features are enabled). The sort keys are on the input of apply, so the rows from the same input row are ties. The
 * LIMITs are chosen so that the ties are not split, and the rows are compared as multiset in addition to the order
 * of the sort keys. The parameter is use_rtx.
 */
class sql_apply_preserves_order_test :
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
        cfg_->scan_block_size(5);
        cfg_->scan_yield_interval(0);
        db_setup(cfg_);
        register_functions();
        create_tables();
    }

    void TearDown() override {
        unregister_functions();
        db_teardown();
    }

    void register_functions() {
        namespace t = takatori::type;
        decl_tn_a_ = global::regular_function_provider()->add(std::make_shared<yugawara::function::declaration>(
            tvf_id_tn_a,
            "tn_a",
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
            tvf_id_tn_a,
            std::make_shared<executor::function::table_valued_function_info>(
                executor::function::table_valued_function_kind::builtin,
                tn_a,
                1,
                executor::function::table_valued_function_info::columns_type{
                    executor::function::table_valued_function_column{"c1"},
                }
            )
        );
    }

    void unregister_functions() {
        global::table_valued_function_repository().clear();
        if(decl_tn_a_) {
            global::regular_function_provider()->remove(*decl_tn_a_);
            decl_tn_a_.reset();
        }
    }

    // t: k in 0..59, v = k * 7 % 60 (unique) with the index t_v, w = k % 7
    void create_tables() {
        execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v INT, w INT)");
        execute_statement("CREATE INDEX t_v ON t (v)");
        std::string values{};
        for(std::int32_t i = 0; i < 60; ++i) {
            auto k = (i * 37) % 60;
            values += (i == 0 ? "" : ", ");
            values += "(" + std::to_string(k) + ", " + std::to_string(k * 7 % 60) + ", " + std::to_string(k % 7) + ")";
        }
        execute_statement("INSERT INTO t VALUES " + values);
    }

    std::vector<basic_record> query(std::string_view sql, bool preserves_order) {
        cfg_->enable_apply_preserves_order(preserves_order);
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

    bool has_sort(std::string_view sql, bool preserves_order) {
        cfg_->enable_apply_preserves_order(preserves_order);
        std::string plan{};
        explain_statement(sql, plan);
        return plan.find(R"("kind":"group")") != std::string::npos;
    }

    static std::vector<std::optional<std::int32_t>> sort_keys(
        std::vector<basic_record> const& records,
        std::size_t column
    ) {
        std::vector<std::optional<std::int32_t>> ret{};
        ret.reserve(records.size());
        for(auto&& rec : records) {
            ret.emplace_back(rec.is_null(column) ? std::nullopt : std::optional{rec.get_value<std::int32_t>(column)});
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

    // verifies the query has the same rows and the same order of the sort key (the column at sort_column) with and
    // without apply_preserves_order. If expect_sort_removed is true, it also verifies the sort is removed only with
    // apply_preserves_order (but the plan can change).
    void verify(std::string_view sql, std::size_t sort_column, bool expect_sort_removed = true) {
        SCOPED_TRACE(sql);
        if(expect_sort_removed) {
            EXPECT_TRUE(has_sort(sql, false));
            EXPECT_FALSE(has_sort(sql, true));
        }
        auto expected = query(sql, false);
        auto actual = query(sql, true);
        EXPECT_FALSE(expected.empty());
        EXPECT_EQ(sorted(expected), sorted(actual)) << "expected:\n" << to_string(expected)
            << "actual:\n" << to_string(actual);
        EXPECT_EQ(sort_keys(expected, sort_column), sort_keys(actual, sort_column)) << "expected:\n"
            << to_string(expected) << "actual:\n" << to_string(actual);
    }

    std::shared_ptr<configuration> cfg_{};  //NOLINT
    std::shared_ptr<yugawara::function::declaration> decl_tn_a_{};  //NOLINT
};

INSTANTIATE_TEST_SUITE_P(
    apply_preserves_order,
    sql_apply_preserves_order_test,
    ::testing::Bool(),
    [](auto const& info) {
        return std::string{info.param ? "rtx" : "occ"};
    }
);

// the rows for k = 0, 1, 2, 3, 4, 5, 6 are 2, 0, 1, 2, 0, 1, 2 rows for CROSS APPLY, and 2, 1, 1, 2, 1, 1, 2 rows
// for OUTER APPLY

TEST_P(sql_apply_preserves_order_test, cross_apply) {
    verify("SELECT t.k, r.c1 FROM t CROSS APPLY tn_a(t.k) AS r(c1) ORDER BY t.k LIMIT 3", 0);
    verify("SELECT t.k, r.c1 FROM t CROSS APPLY tn_a(t.k) AS r(c1) ORDER BY t.k LIMIT 8", 0);
    verify("SELECT t.k, r.c1 FROM t CROSS APPLY tn_a(t.k) AS r(c1) ORDER BY t.k", 0);
}

TEST_P(sql_apply_preserves_order_test, outer_apply) {
    verify("SELECT t.k, r.c1 FROM t OUTER APPLY tn_a(t.k) AS r(c1) ORDER BY t.k LIMIT 4", 0);
    verify("SELECT t.k, r.c1 FROM t OUTER APPLY tn_a(t.k) AS r(c1) ORDER BY t.k LIMIT 10", 0);
    verify("SELECT t.k, r.c1 FROM t OUTER APPLY tn_a(t.k) AS r(c1) ORDER BY t.k", 0);
}

TEST_P(sql_apply_preserves_order_test, backward) {
    // the rows for k = 59, 58, 57, 56 are 1, 0, 2, 1 rows for CROSS APPLY
    verify("SELECT t.k, r.c1 FROM t CROSS APPLY tn_a(t.k) AS r(c1) ORDER BY t.k DESC LIMIT 4", 0);
    verify("SELECT t.k, r.c1 FROM t OUTER APPLY tn_a(t.k) AS r(c1) ORDER BY t.k DESC", 0);
}

TEST_P(sql_apply_preserves_order_test, filter) {
    // the scan has no row count because of the filter
    verify("SELECT t.k, r.c1 FROM t CROSS APPLY tn_a(t.k) AS r(c1) WHERE t.w > 1 ORDER BY t.k LIMIT 3", 0);
    verify("SELECT t.k, r.c1 FROM t CROSS APPLY tn_a(t.k) AS r(c1) WHERE r.c1 > 100 ORDER BY t.k LIMIT 4", 0);
}

TEST_P(sql_apply_preserves_order_test, secondary_index) {
    // the rows for v = 0, 1, 2, 3 (k = 0, 43, 26, 9) are 2, 0, 1, 2 rows for CROSS APPLY
    // CROSS APPLY can drop rows, so the scan has no row count, and the compiler does not choose t_v but sorts the
    // primary index, because reading the whole secondary index with the primary index lookups is estimated more
    // costly than the sort (O2 in the plan catalog)
    verify("SELECT t.v, r.c1 FROM t CROSS APPLY tn_a(t.k) AS r(c1) ORDER BY t.v LIMIT 5", 0, false);
    // OUTER APPLY outputs at least one row for each input row, so the scan of t_v has the row count
    verify("SELECT t.v, r.c1 FROM t OUTER APPLY tn_a(t.k) AS r(c1) ORDER BY t.v LIMIT 5", 0);
}

TEST_P(sql_apply_preserves_order_test, not_optimized) {
    // the sort key uses the output of the function
    verify("SELECT t.k, r.c1 FROM t CROSS APPLY tn_a(t.k) AS r(c1) ORDER BY r.c1 LIMIT 5", 1, false);
    verify("SELECT t.k, r.c1 FROM t CROSS APPLY tn_a(t.k) AS r(c1) ORDER BY t.k, r.c1 LIMIT 5", 1, false);
}

}  // namespace jogasaki::testing
