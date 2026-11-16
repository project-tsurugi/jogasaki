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

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/writer_count_calculator.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/custom_any_sequence_stream.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::mock;
using namespace std::string_view_literals;

namespace {

constexpr std::size_t tvf_id_tn_y = 12400;

/**
 * @brief mock table-valued function `tn_y(x INT) -> TABLE(c1 INT)` that yields once before returning its rows
 * @details returns not_ready on the first call (so that the apply operator yields), and then returns 2 rows
 * (x * 10, x * 10 + 1).
 */
std::unique_ptr<data::any_sequence_stream> tn_y(
    executor::expr::evaluator_context& /* ctx */,
    takatori::util::sequence_view<data::any> args
) {
    auto x = args[0].to<std::int32_t>();
    return std::make_unique<custom_any_sequence_stream>(
        [x, n = 0](data::any_sequence& seq) mutable -> data::any_sequence_stream::status_type {
            auto call = n++;
            if (call == 0) {
                return data::any_sequence_stream::status_type::not_ready;
            }
            if (call <= 2) {
                seq = data::any_sequence{data::any_sequence::storage_type{
                    data::any{std::in_place_type<std::int32_t>, x * 10 + (call - 1)}
                }};
                return data::any_sequence_stream::status_type::ok;
            }
            return data::any_sequence_stream::status_type::end_of_stream;
        }
    );
}

}  // namespace

/**
 * @brief tests that the ordered emit keeps the order even if the task yields in the middle (tsurugi-issues#1589)
 * @details the emit with preserve_output can be in the same process as the operators which yield (scan and apply).
 * The result records must be in order even if the process task yields and resumes, and there are multiple writers
 * available for the request. The parameter is use_rtx.
 */
class sql_ordered_emit_test :
    public ::testing::TestWithParam<bool>,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        cfg_ = std::make_shared<configuration>();
        // multiple writers are available for the request
        cfg_->max_result_set_writers(4);
        cfg_->scan_default_parallel(4);
        // yield frequently
        cfg_->scan_block_size(5);
        cfg_->scan_yield_interval(0);
        db_setup(cfg_);
        register_functions();
        execute_statement("CREATE TABLE t (k INT PRIMARY KEY, v INT)");
        std::string values{};
        for(std::int32_t i = 0; i < row_count; ++i) {
            auto k = (i * 37) % row_count;
            values += (i == 0 ? "" : ", ");
            values += "(" + std::to_string(k) + ", " + std::to_string(k * 2) + ")";
        }
        execute_statement("INSERT INTO t VALUES " + values);
    }

    void TearDown() override {
        unregister_functions();
        db_teardown();
    }

    void register_functions() {
        namespace t = takatori::type;
        decl_tn_y_ = global::regular_function_provider()->add(std::make_shared<yugawara::function::declaration>(
            tvf_id_tn_y,
            "tn_y",
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
            tvf_id_tn_y,
            std::make_shared<executor::function::table_valued_function_info>(
                executor::function::table_valued_function_kind::builtin,
                tn_y,
                1,
                executor::function::table_valued_function_info::columns_type{
                    executor::function::table_valued_function_column{"c1"},
                }
            )
        );
    }

    void unregister_functions() {
        global::table_valued_function_repository().clear();
        if(decl_tn_y_) {
            global::regular_function_provider()->remove(*decl_tn_y_);
            decl_tn_y_.reset();
        }
    }

    std::vector<basic_record> query(std::string_view sql) {
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

    // returns true if the plan has no group exchange, i.e. the order is given by the scan, not by the sort
    bool sort_removed(std::string_view sql) {
        std::string plan{};
        explain_statement(sql, plan);
        return plan.find(R"("kind":"group")") == std::string::npos;
    }

    static std::string to_string(std::vector<basic_record> const& records) {
        std::stringstream ss{};
        for(auto&& rec : records) {
            ss << rec << "\n";
        }
        return ss.str();
    }

    // verifies the query returns the expected values of the first column in this order
    void verify(std::string_view sql, std::vector<std::int32_t> const& expected) {
        SCOPED_TRACE(sql);
        EXPECT_TRUE(sort_removed(sql));
        auto result = query(sql);
        std::vector<std::int32_t> actual{};
        actual.reserve(result.size());
        for(auto&& rec : result) {
            actual.emplace_back(rec.get_value<std::int32_t>(0));
        }
        EXPECT_EQ(expected, actual) << to_string(result);
    }

    static constexpr std::int32_t row_count = 100;

    std::shared_ptr<configuration> cfg_{};  //NOLINT
    std::shared_ptr<yugawara::function::declaration> decl_tn_y_{};  //NOLINT
};

INSTANTIATE_TEST_SUITE_P(
    ordered_emit,
    sql_ordered_emit_test,
    ::testing::Bool(),
    [](auto const& info) {
        return std::string{info.param ? "rtx" : "occ"};
    }
);

TEST_P(sql_ordered_emit_test, scan_yields) {
    // the scan yields every 5 records
    std::vector<std::int32_t> asc{};
    std::vector<std::int32_t> desc{};
    for(std::int32_t k = 0; k < row_count; ++k) {
        asc.emplace_back(k);
        desc.emplace_back(row_count - 1 - k);
    }
    verify("SELECT k FROM t ORDER BY k", asc);
    verify("SELECT k FROM t ORDER BY k DESC", desc);
}

TEST_P(sql_ordered_emit_test, apply_yields) {
    // the apply yields for each input record, while the scan does not yield
    cfg_->scan_block_size(0);
    std::vector<std::int32_t> expected{};
    for(std::int32_t k = 0; k < row_count; ++k) {
        expected.emplace_back(k * 10);
        expected.emplace_back(k * 10 + 1);
    }
    verify("SELECT r.c1 FROM t CROSS APPLY tn_y(t.k) AS r(c1) ORDER BY t.k", expected);
}

TEST_P(sql_ordered_emit_test, max_writer_count) {
    // the number of writers for the server request is 1 for the ordered relation, while it is the number of
    // partitions of the parallel scan for the unordered relation in RTX
    if(! GetParam()) {
        GTEST_SKIP() << "the scan is not parallel in OCC";
    }
    auto tx = utils::create_transaction(*db_, true, false);
    auto tctx = api::get_transaction_context(*tx);
    auto writer_count = [&](std::string_view sql) -> std::optional<std::size_t> {
        std::unique_ptr<api::executable_statement> stmt{};
        EXPECT_EQ(status::ok, db_->create_executable(sql, stmt));
        return executor::calculate_max_writer_count(*stmt, *tctx);
    };
    EXPECT_EQ(1, writer_count("SELECT k FROM t ORDER BY k"));
    EXPECT_EQ(1, writer_count("SELECT r.c1 FROM t CROSS APPLY tn_y(t.k) AS r(c1) ORDER BY t.k"));
    EXPECT_EQ(4, writer_count("SELECT k FROM t"));
    EXPECT_EQ(status::ok, tx->commit());
}

}  // namespace jogasaki::testing
