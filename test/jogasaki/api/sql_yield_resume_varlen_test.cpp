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

// This file is the CHAR(20) counterpart of sql_yield_resume_test.cpp.
// It validates the same yield/resume scenarios for all relational operators that appear
// upstream of an APPLY operator, but uses CHAR(20) columns throughout so that the
// variable-length (varlen) buffer code paths are exercised during yield and resume.

#include <algorithm>
#include <atomic>
#include <chrono>
#include <future>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/type/character.h>
#include <takatori/type/table.h>
#include <takatori/type/varying.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::mock;
using takatori::util::sequence_view;
using executor::expr::evaluator_context;
using namespace executor::function;
namespace t = takatori::type;

using kind = meta::field_type_kind;

// ---------------------------------------------------------------------------
// 20-character string constants used as table column values and TVF output.
// All values are exactly 20 characters to fit in CHAR(20) without padding.
// ---------------------------------------------------------------------------
static constexpr char kc0_1[] = "11111111111111111111";  // 20 '1's: primary-key value, row 1
static constexpr char kc0_2[] = "22222222222222222222";  // 20 '2's: primary-key value, row 2
static constexpr char kc0_3[] = "33333333333333333333";  // 20 '3's: primary-key value, row 3
static constexpr char kc1_a[] = "aaaaaaaaaaaaaaaaaaaa";  // 20 'a's: C1 value, row 1
static constexpr char kc1_b[] = "bbbbbbbbbbbbbbbbbbbb";  // 20 'b's: C1 value, row 2
static constexpr char kc1_A[] = "AAAAAAAAAAAAAAAAAAAA";  // 20 'A's: UPPER(kc1_a)
static constexpr char kc1_B[] = "BBBBBBBBBBBBBBBBBBBB";  // 20 'B's: UPPER(kc1_b)
static constexpr char ktvf_r[] = "rrrrrrrrrrrrrrrrrrrr";  // 20 'r's: fixed TVF return value

/**
 * @brief blocking stream that returns accessor::text when an atomic flag is set.
 * @details returns not_ready while the flag is false, then delivers one CHAR(20) row
 *          of (c1 = ktvf_r) and then end_of_stream.  The backing string for the text
 *          value is a string literal (static storage), so no resource allocation is needed.
 */
class blocking_any_sequence_stream_char : public data::any_sequence_stream {
public:
    explicit blocking_any_sequence_stream_char(std::shared_ptr<std::atomic_bool> flag) :
        flag_(std::move(flag))
    {}

    [[nodiscard]] status_type try_next(data::any_sequence& sequence) override {
        if (! flag_->load()) {
            return status_type::not_ready;
        }
        if (delivered_) {
            return status_type::end_of_stream;
        }
        sequence = data::any_sequence{data::any_sequence::storage_type{
            data::any{std::in_place_type<accessor::text>, accessor::text{ktvf_r}}
        }};
        delivered_ = true;
        return status_type::ok;
    }

    [[nodiscard]] status_type next(
        data::any_sequence& sequence,
        std::optional<std::chrono::milliseconds> /* timeout */) override {
        return try_next(sequence);
    }

    void close() override {}

private:
    std::shared_ptr<std::atomic_bool> flag_{};
    bool delivered_{false};
};

namespace {

[[nodiscard]] bool contains(std::string_view whole, std::string_view part) noexcept {
    return whole.find(part) != std::string_view::npos;
}

/// @brief field type for CHAR(20) — fixed-length character with length 20.
[[nodiscard]] meta::field_type char20_ft() {
    return meta::field_type{std::make_shared<meta::character_field_option>(false, std::size_t{20})};
}

/// @brief field type for VARCHAR(*) — unbounded varying-length character (TVF output / UPPER result).
[[nodiscard]] meta::field_type varchar_ft() {
    return meta::field_type{std::make_shared<meta::character_field_option>(true, std::nullopt)};
}

/// @brief field type for VARCHAR(20) — varying-length character with max length 20 (UNION ALL result).
[[nodiscard]] meta::field_type varchar20_ft() {
    return meta::field_type{std::make_shared<meta::character_field_option>(true, std::size_t{20})};
}

}  // namespace

/**
 * @brief fixture for yield/resume tests with CHAR(20) varlen data.
 * @details identical in structure to sql_yield_resume_test but uses CHAR(20) columns
 *          everywhere so that the varlen buffer code paths inside each operator are
 *          exercised during yield and resume.
 */
class sql_yield_resume_varlen_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->thread_pool_size(1);
        db_setup(cfg);
        utils::set_global_tx_option(utils::create_tx_option{false, true});
    }

    void TearDown() override {
        utils::set_global_tx_option(utils::create_tx_option{});
        global::table_valued_function_repository().clear();
        db_teardown();
    }

    /**
     * @brief common helper that verifies yield propagation with CHAR(20) varlen data.
     * @details registers a blocking TVF named tvf_varlen_blocking that accepts VARCHAR and
     *          returns one CHAR(20) column.  the TVF blocks until an atomic flag is set,
     *          so that the apply operator yields the worker thread.  the helper then runs
     *          the query asynchronously and measures how long a concurrent INSERT takes.
     *          if yield propagation works correctly the INSERT completes while the TVF is
     *          still blocking (well under 200 ms).
     * @param query SQL to execute; must reference tvf_varlen_blocking.
     * @param expected_plan_operators if non-empty, ASSERT that explain output contains each string.
     * @param expected_results expected rows returned by the query (verified when non-empty).
     */
    void run_yield_propagation_test(
        std::string_view query,
        std::vector<std::string> const& expected_plan_operators = {},
        std::vector<mock::basic_record> const& expected_results = {}
    ) {
        execute_statement("CREATE TABLE T2 (C0 INT PRIMARY KEY)");

        auto unblock_flag = std::make_shared<std::atomic_bool>(false);

        constexpr std::size_t tvf_id_blocking = 13100;

        auto mock_tvf = [unblock_flag](
            evaluator_context& /* ctx */,
            sequence_view<data::any> /* args */
        ) -> std::unique_ptr<data::any_sequence_stream> {
            return std::make_unique<blocking_any_sequence_stream_char>(unblock_flag);
        };

        auto decl_blocking = global::regular_function_provider()->add(
            std::make_shared<yugawara::function::declaration>(
                tvf_id_blocking,
                "tvf_varlen_blocking",
                std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                    {"c1", std::make_shared<t::character>(t::varying)},
                }),
                std::vector<std::shared_ptr<takatori::type::data const>>{
                    std::make_shared<t::character>(t::varying)
                },
                yugawara::function::declaration::feature_set_type{
                    yugawara::function::function_feature::table_valued_function
                }
            )
        );

        global::table_valued_function_repository().add(
            tvf_id_blocking,
            std::make_shared<table_valued_function_info>(
                table_valued_function_kind::builtin,
                mock_tvf,
                1,
                table_valued_function_info::columns_type{
                    table_valued_function_column{"c1"}
                }
            )
        );

        if (! expected_plan_operators.empty()) {
            std::string plan{};
            explain_statement(query, plan);
            for (auto const& op : expected_plan_operators) {
                ASSERT_TRUE(contains(plan, op))
                    << "expected plan to contain '" << op
                    << "' but got: " << plan;
            }
        }

        std::vector<mock::basic_record> result{};
        auto query_future = std::async(std::launch::async, [&]() {
            execute_query(query, result);
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        std::atomic<long> insert_duration_ms{-1};
        auto insert_future = std::async(std::launch::async, [&]() {
            auto insert_start = std::chrono::steady_clock::now();
            execute_statement("INSERT INTO T2 VALUES (1)");
            insert_duration_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - insert_start).count());
        });

        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        unblock_flag->store(true);

        insert_future.get();
        query_future.get();

        if (decl_blocking) {
            global::regular_function_provider()->remove(*decl_blocking);
        }

        EXPECT_LT(insert_duration_ms.load(), 200);

        if (! expected_results.empty()) {
            ASSERT_EQ(expected_results.size(), result.size());
            auto sorted_result = result;
            auto sorted_expected = expected_results;
            std::sort(sorted_result.begin(), sorted_result.end());
            std::sort(sorted_expected.begin(), sorted_expected.end());
            for (std::size_t i = 0; i < sorted_expected.size(); ++i) {
                EXPECT_EQ(sorted_expected[i], sorted_result[i]);
            }
        }
    }
};

TEST_F(sql_yield_resume_varlen_test, scan) {
    // varlen version of sql_yield_resume_test::scan.
    // plan: scan(T) -> apply(TVF(T.C0))
    // T rows contain CHAR(20) fields; scan reads them into varlen buffers.
    execute_statement("CREATE TABLE T (C0 CHAR(20) PRIMARY KEY, C1 CHAR(20))");
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', 'aaaaaaaaaaaaaaaaaaaa')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', 'bbbbbbbbbbbbbbbbbbbb')");

    run_yield_propagation_test(
        "SELECT T.C0, T.C1, tvf.c1 "
        "FROM T CROSS APPLY tvf_varlen_blocking(T.C0) AS tvf",
        {},
        {
            mock::typed_nullable_record<kind::character, kind::character, kind::character>(
                std::make_tuple(char20_ft(), char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_1}, accessor::text{kc1_a}, accessor::text{ktvf_r})),
            mock::typed_nullable_record<kind::character, kind::character, kind::character>(
                std::make_tuple(char20_ft(), char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_2}, accessor::text{kc1_b}, accessor::text{ktvf_r})),
        }
    );
}

TEST_F(sql_yield_resume_varlen_test, filter) {
    // varlen version of sql_yield_resume_test::filter.
    // plan: scan(T) -> filter(T.C0 <> T.C1) -> apply(TVF)
    // the comparison of two different CHAR(20) columns cannot be folded into the scan range,
    // so the planner places an explicit filter operator.  the filter reads CHAR(20) values
    // from varlen buffers in its predicate evaluation.
    execute_statement("CREATE TABLE T (C0 CHAR(20) PRIMARY KEY, C1 CHAR(20))");
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', 'aaaaaaaaaaaaaaaaaaaa')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', 'bbbbbbbbbbbbbbbbbbbb')");
    // this row has C0 = C1, so it is filtered out by the predicate below
    execute_statement("INSERT INTO T VALUES ('33333333333333333333', '33333333333333333333')");

    run_yield_propagation_test(
        "SELECT T.C0, T.C1, tvf.c1 "
        "FROM T CROSS APPLY tvf_varlen_blocking(T.C0) AS tvf "
        "WHERE T.C0 <> T.C1",
        {"filter"},
        {
            mock::typed_nullable_record<kind::character, kind::character, kind::character>(
                std::make_tuple(char20_ft(), char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_1}, accessor::text{kc1_a}, accessor::text{ktvf_r})),
            mock::typed_nullable_record<kind::character, kind::character, kind::character>(
                std::make_tuple(char20_ft(), char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_2}, accessor::text{kc1_b}, accessor::text{ktvf_r})),
        }
    );
}

TEST_F(sql_yield_resume_varlen_test, project) {
    // varlen version of sql_yield_resume_test::project.
    // plan: scan(T) -> project(UPPER(T.C1) AS C) -> apply(TVF(S.C))
    // UPPER applied to a CHAR(20) column produces a new CHAR(20) value; the project operator
    // reads and writes varlen buffers when computing and staging the result.
    execute_statement("CREATE TABLE T (C0 CHAR(20) PRIMARY KEY, C1 CHAR(20))");
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', 'aaaaaaaaaaaaaaaaaaaa')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', 'bbbbbbbbbbbbbbbbbbbb')");

    run_yield_propagation_test(
        "SELECT S.C, tvf.c1 "
        "FROM (SELECT UPPER(T.C1) AS C FROM T) S CROSS APPLY tvf_varlen_blocking(S.C) AS tvf",
        {"project"},
        {
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(varchar_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc1_A}, accessor::text{ktvf_r})),
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(varchar_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc1_B}, accessor::text{ktvf_r})),
        }
    );
}

TEST_F(sql_yield_resume_varlen_test, join_scan) {
    // varlen version of sql_yield_resume_test::join_scan.
    // plan: scan(T1) -> join_scan(T2 on T1.C1=T2.C0) -> apply(TVF)
    // T has a composite primary key (C0, C1); joining on T1.C1 = T2.C0 matches only the
    // first component of T2's composite key, so the planner uses join_scan (range scan).
    // join_scan reads CHAR(20) join-key values from varlen buffers for range construction.
    execute_statement("CREATE TABLE T (C0 CHAR(20), C1 CHAR(20), PRIMARY KEY(C0, C1))");
    // rows form a cycle: row N's C1 equals row (N mod 3 + 1)'s C0
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', '22222222222222222222')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', '33333333333333333333')");
    execute_statement("INSERT INTO T VALUES ('33333333333333333333', '11111111111111111111')");

    run_yield_propagation_test(
        "SELECT R.C0, tvf.c1 "
        "FROM (SELECT T1.C0 FROM T T1 JOIN T T2 ON T1.C1 = T2.C0) R "
        "CROSS APPLY tvf_varlen_blocking(R.C0) AS tvf",
        {"join_scan"},
        {
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_1}, accessor::text{ktvf_r})),
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_2}, accessor::text{ktvf_r})),
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_3}, accessor::text{ktvf_r})),
        }
    );
}

TEST_F(sql_yield_resume_varlen_test, join_find) {
    // varlen version of sql_yield_resume_test::join_find.
    // plan: scan(T1) -> join_find(T2 on T1.C1=T2.C0) -> apply(TVF)
    // joining on T1.C1 = T2.C0 allows a primary-key point lookup for each T1 row,
    // producing a join_find operator.  the lookup key is a CHAR(20) value read from
    // a varlen buffer, exercising the varlen buffer path in join_find.
    execute_statement("CREATE TABLE T (C0 CHAR(20) PRIMARY KEY, C1 CHAR(20))");
    // C1 of each row equals C0 of the other row, so every row has one join partner
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', '22222222222222222222')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', '11111111111111111111')");

    run_yield_propagation_test(
        "SELECT R.C0, tvf.c1 "
        "FROM (SELECT T1.C0 FROM T T1 JOIN T T2 ON T1.C1 = T2.C0) R "
        "CROSS APPLY tvf_varlen_blocking(R.C0) AS tvf",
        {"join_find"},
        {
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_1}, accessor::text{ktvf_r})),
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_2}, accessor::text{ktvf_r})),
        }
    );
}

TEST_F(sql_yield_resume_varlen_test, take_group_and_flatten) {
    // varlen version of sql_yield_resume_test::take_group_and_flatten.
    // plan: scan(T) -> offer(C1) -> group-exchange -> take_group -> flatten -> apply(TVF)
    // GROUP BY C1 on a CHAR(20) column forces take_group + flatten operators; both must
    // correctly propagate yield and handle CHAR(20) group-key values in their varlen buffers.
    execute_statement("CREATE TABLE T (C0 CHAR(20) PRIMARY KEY, C1 CHAR(20))");
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', 'aaaaaaaaaaaaaaaaaaaa')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', 'bbbbbbbbbbbbbbbbbbbb')");

    run_yield_propagation_test(
        "SELECT R.C1, tvf.c1 "
        "FROM (SELECT C1 FROM T GROUP BY C1) R "
        "CROSS APPLY tvf_varlen_blocking(R.C1) AS tvf",
        {"take_group", "flatten"},
        {
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc1_a}, accessor::text{ktvf_r})),
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc1_b}, accessor::text{ktvf_r})),
        }
    );
}

TEST_F(sql_yield_resume_varlen_test, take_cogroup_and_join) {
    // varlen version of sql_yield_resume_test::take_cogroup_and_join.
    // plan: scan(T1), scan(T2) -> offer(C1) -> cogroup-exchange ->
    //       take_cogroup -> join(T1.C1=T2.C1) -> apply(TVF)
    // the self-join on the non-primary-key CHAR(20) column C1 introduces a sort-merge join
    // via take_cogroup + join operators.  both operators must correctly propagate yield and
    // preserve CHAR(20) sort-key values across varlen buffers.
    execute_statement("CREATE TABLE T (C0 CHAR(20) PRIMARY KEY, C1 CHAR(20))");
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', 'aaaaaaaaaaaaaaaaaaaa')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', 'bbbbbbbbbbbbbbbbbbbb')");

    run_yield_propagation_test(
        "SELECT R.C0, tvf.c1 "
        "FROM (SELECT T1.C0 FROM T T1 JOIN T T2 ON T1.C1 = T2.C1) R "
        "CROSS APPLY tvf_varlen_blocking(R.C0) AS tvf",
        {"take_cogroup", "join"},
        {
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_1}, accessor::text{ktvf_r})),
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_2}, accessor::text{ktvf_r})),
        }
    );
}

TEST_F(sql_yield_resume_varlen_test, take_group_and_aggregate_group) {
    // varlen version of sql_yield_resume_test::take_group_and_aggregate_group.
    // plan: scan(T) -> offer -> group-exchange -> take_group -> aggregate_group -> apply(TVF)
    // MAX over a CHAR(20) column introduces take_group + aggregate_group operators; both must
    // correctly propagate yield and handle CHAR(20) aggregate values in their varlen buffers.
    execute_statement("CREATE TABLE T (C0 CHAR(20) PRIMARY KEY, C1 CHAR(20))");
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', 'aaaaaaaaaaaaaaaaaaaa')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', 'bbbbbbbbbbbbbbbbbbbb')");

    // MAX(C1) returns 'bbb...b' (alphabetically greater than 'aaa...a')
    run_yield_propagation_test(
        "SELECT R.C, tvf.c1 "
        "FROM (SELECT MAX(C1) AS C FROM T) R "
        "CROSS APPLY tvf_varlen_blocking(R.C) AS tvf",
        {"take_group", "flatten_group"},
        {
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(varchar_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc1_b}, accessor::text{ktvf_r})),
        }
    );
}

TEST_F(sql_yield_resume_varlen_test, take_flat) {
    // varlen version of sql_yield_resume_test::take_flat.
    // plan: take_flat(UNION ALL of two scans) -> apply(TVF)
    // UNION ALL forces a forward relay step consumed via take_flat; the operator must
    // correctly propagate yield while holding CHAR(20) values in its varlen buffer.
    execute_statement("CREATE TABLE T (C0 CHAR(20) PRIMARY KEY, C1 CHAR(20))");
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', 'aaaaaaaaaaaaaaaaaaaa')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', 'bbbbbbbbbbbbbbbbbbbb')");

    run_yield_propagation_test(
        "SELECT R.C0, tvf.c1 "
        "FROM (SELECT C0 FROM T WHERE C0 = '11111111111111111111' "
              "UNION ALL "
              "SELECT C0 FROM T WHERE C0 = '22222222222222222222') R "
        "CROSS APPLY tvf_varlen_blocking(R.C0) AS tvf",
        {"take_flat"},
        {
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(varchar20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_1}, accessor::text{ktvf_r})),
            mock::typed_nullable_record<kind::character, kind::character>(
                std::make_tuple(varchar20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_2}, accessor::text{ktvf_r})),
        }
    );
}

TEST_F(sql_yield_resume_varlen_test, find) {
    // varlen version of sql_yield_resume_test::find.
    // plan: find(T where C0='11..1') -> apply(TVF(T.C0))
    // the WHERE clause on the CHAR(20) primary key causes the planner to emit a find operator
    // (primary-key point lookup) instead of scan.  find reads a CHAR(20) key from the varlen
    // buffer and must correctly propagate yield from the downstream apply operator.
    execute_statement("CREATE TABLE T (C0 CHAR(20) PRIMARY KEY, C1 CHAR(20))");
    execute_statement("INSERT INTO T VALUES ('11111111111111111111', 'aaaaaaaaaaaaaaaaaaaa')");
    execute_statement("INSERT INTO T VALUES ('22222222222222222222', 'bbbbbbbbbbbbbbbbbbbb')");

    run_yield_propagation_test(
        "SELECT T.C0, T.C1, tvf.c1 "
        "FROM T CROSS APPLY tvf_varlen_blocking(T.C0) AS tvf "
        "WHERE T.C0 = '11111111111111111111'",
        {"find"},
        {
            mock::typed_nullable_record<kind::character, kind::character, kind::character>(
                std::make_tuple(char20_ft(), char20_ft(), varchar_ft()),
                std::make_tuple(accessor::text{kc0_1}, accessor::text{kc1_a}, accessor::text{ktvf_r})),
        }
    );
}

}  // namespace jogasaki::testing
