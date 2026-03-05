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
#include <atomic>
#include <chrono>
#include <cstdint>
#include <future>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <thread>
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
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/mock_any_sequence_stream.h>
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

/**
 * @brief stream that returns not_ready until an atomic flag is set, then delivers one row.
 * @details used in tests to simulate a TVF stream that is not immediately ready,
 *          allowing the apply operator to yield the worker thread.
 */
class blocking_any_sequence_stream : public data::any_sequence_stream {
public:
    /**
     * @brief constructs the stream.
     * @param flag the flag to check; returns not_ready while false, then delivers one row.
     * @param v1 int32 value for the first column of the delivered row.
     * @param v2 int64 value for the second column of the delivered row.
     */
    blocking_any_sequence_stream(
        std::shared_ptr<std::atomic_bool> flag,
        std::int32_t v1,
        std::int64_t v2
    ) : flag_(std::move(flag)), v1_(v1), v2_(v2) {}

    [[nodiscard]] status_type try_next(data::any_sequence& sequence) override {
        if (! flag_->load()) {
            return status_type::not_ready;
        }
        if (delivered_) {
            return status_type::end_of_stream;
        }
        sequence = data::any_sequence{data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int32_t>, v1_},
            data::any{std::in_place_type<std::int64_t>, v2_}
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
    std::int32_t v1_{};
    std::int64_t v2_{};
    bool delivered_{false};
};

namespace {

[[nodiscard]] bool contains(std::string_view whole, std::string_view part) noexcept {
    return whole.find(part) != std::string_view::npos;
}

}  // namespace

/**
 * @brief test relational operatos correctly resumes after downstream apply operator yields due to blocked TVF.
 */
class sql_yield_resume_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        // use a single worker thread to force contention between the blocked TVF and INSERT
        auto cfg = std::make_shared<configuration>();
        cfg->thread_pool_size(1);
        db_setup(cfg);
        // use OCC (short) transactions so that SELECT and INSERT can run concurrently.
        // LTX (the default) would serialize them via write_preserve conflict,
        // blocking INSERT until SELECT completes regardless of yield.
        utils::set_global_tx_option(utils::create_tx_option{false, true});
    }

    void TearDown() override {
        // restore default transaction option
        utils::set_global_tx_option(utils::create_tx_option{});
        global::table_valued_function_repository().clear();
        db_teardown();
    }

    /**
     * @brief common helper that verifies yield propagation for an operator upstream of apply.
     * @details registers a blocking TVF named mock_table_func_blocking, optionally verifies
     *          that the query plan contains an expected operator keyword, then executes the
     *          given query asynchronously while concurrently measuring INSERT timing.
     *          if the operator under test correctly propagates yield from the apply operator,
     *          the worker thread is released while the TVF is blocking and the INSERT completes
     *          well before the TVF unblocks (~200ms).
     * @param query SQL to execute; must reference mock_table_func_blocking.
     * @param expected_plan_operators if non-empty, ASSERT that explain output contains each string.
     * @param expected_results expected rows returned by the query (verified when non-empty).
     */
    void run_yield_propagation_test(
        std::string_view query,
        std::vector<std::string> const& expected_plan_operators = {},
        std::vector<mock::basic_record> const& expected_results = {}
    ) {
        execute_statement("CREATE TABLE T2 (C0 INT PRIMARY KEY)");

        // atomic flag: TVF blocks until this is set to true
        auto unblock_flag = std::make_shared<std::atomic_bool>(false);

        constexpr std::size_t tvf_id_blocking = 13000;

        auto mock_table_func_blocking = [unblock_flag](
            evaluator_context& /* ctx */,
            sequence_view<data::any> /* args */
        ) -> std::unique_ptr<data::any_sequence_stream> {
            return std::make_unique<blocking_any_sequence_stream>(unblock_flag, 1, 100);
        };

        auto decl_blocking = global::regular_function_provider()->add(
            std::make_shared<yugawara::function::declaration>(
                tvf_id_blocking,
                "mock_table_func_blocking",
                std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                    {"c1", std::make_shared<t::int4>()},
                    {"c2", std::make_shared<t::int8>()},
                }),
                std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::int4>()},
                yugawara::function::declaration::feature_set_type{
                    yugawara::function::function_feature::table_valued_function
                }
            )
        );

        global::table_valued_function_repository().add(
            tvf_id_blocking,
            std::make_shared<table_valued_function_info>(
                table_valued_function_kind::builtin,
                mock_table_func_blocking,
                1,
                table_valued_function_info::columns_type{
                    table_valued_function_column{"c1"},
                    table_valued_function_column{"c2"}
                }
            )
        );

        // verify that the expected operators appear in the query plan before running.
        // if absent, the test premise is violated (a different code path would be tested).
        if (! expected_plan_operators.empty()) {
            std::string plan{};
            explain_statement(query, plan);
            for (auto const& op : expected_plan_operators) {
                ASSERT_TRUE(contains(plan, op))
                    << "expected plan to contain '" << op
                    << "' but got: " << plan;
            }
        }

        // launch the query in background; the single worker enters the TVF and blocks
        std::vector<mock::basic_record> result{};
        auto query_future = std::async(std::launch::async, [&]() {
            execute_query(query, result);
        });

        // wait for the query to start and the TVF to begin blocking the worker
        std::this_thread::sleep_for(std::chrono::milliseconds(50));

        // measure INSERT duration; with yield the worker is freed while TVF blocks
        std::atomic<long> insert_duration_ms{-1};
        auto insert_future = std::async(std::launch::async, [&]() {
            auto insert_start = std::chrono::steady_clock::now();
            execute_statement("INSERT INTO T2 VALUES (1)");
            insert_duration_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::steady_clock::now() - insert_start).count());
        });

        // keep the TVF blocked for 200ms, then release it
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
        unblock_flag->store(true);

        insert_future.get();
        query_future.get();

        if (decl_blocking) {
            global::regular_function_provider()->remove(*decl_blocking);
        }

        // With yield propagation working: worker is released while TVF blocks -> INSERT < 200ms.
        // Without it: worker is stuck inside apply -> INSERT waits for TVF to unblock (~200ms).
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

TEST_F(sql_yield_resume_test, scan) {
    // verify that the apply operator yields the worker thread when the TVF blocks.
    // plan: scan(T) -> apply(TVF(T.C0))
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    run_yield_propagation_test(
        "SELECT T.C0, T.C1, tvf.c1, tvf.c2 "
        "FROM T CROSS APPLY mock_table_func_blocking(T.C0) AS tvf",
        {},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(1, 100, 1, 100),
            create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 200, 1, 100),
        }
    );
}

TEST_F(sql_yield_resume_test, filter) {
    // verify that filter correctly propagates yield from a downstream apply operator
    // plan: scan(T) -> filter(T.C0+T.C1 > 0) -> apply(TVF)
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");
    // row with C0=0, C1=0 gives C0+C1=0, filtered out; rows C0=1 and C0=2 pass (sum > 0)
    execute_statement("INSERT INTO T VALUES (0, 0)");

    // the WHERE T.C0 + T.C1 > 0 expression cannot be folded into the scan range key, so the
    // planner places an explicit filter operator, producing: scan(T) -> filter(T.C0+T.C1 > 0) -> apply(TVF).
    // this exercises the filter operator's yield-propagation path.
    run_yield_propagation_test(
        "SELECT T.C0, T.C1, tvf.c1, tvf.c2 "
        "FROM T CROSS APPLY mock_table_func_blocking(T.C0) AS tvf "
        "WHERE T.C0 + T.C1 > 0",
        {"filter"},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(1, 100, 1, 100),
            create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 200, 1, 100),
        }
    );
}

TEST_F(sql_yield_resume_test, project) {
    // verify that project correctly propagates yield from a downstream apply operator.
    // plan: scan(T) -> project(T.C0+T.C1 AS C) -> apply(TVF(S.C))
    // the subquery computes T.C0+T.C1 AS C, causing the planner to emit a project operator
    // before apply, exercising the project operator's yield-propagation path.
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    run_yield_propagation_test(
        "SELECT S.C, tvf.c1, tvf.c2 "
        "FROM (select T.C0+T.C1 AS C FROM T) S CROSS APPLY mock_table_func_blocking(S.C) AS tvf",
        {"project"},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int8>(101, 1, 100),
            create_nullable_record<kind::int4, kind::int4, kind::int8>(202, 1, 100),
        }
    );
}

TEST_F(sql_yield_resume_test, join_scan) {
    // verify that join_scan correctly propagates yield from a downstream apply operator.
    // plan: scan(T1) -> join_scan(T2 on T1.C1=T2.C0) -> apply(TVF)
    // T has a composite primary key (C0, C1); joining on T1.C1 = T2.C0 matches only
    // the first component of T2's composite key, so the planner uses join_scan (range scan)
    // instead of join_find (point lookup).
    execute_statement("CREATE TABLE T (C0 INT, C1 INT, PRIMARY KEY(C0, C1))");
    execute_statement("INSERT INTO T VALUES (1, 2)");
    execute_statement("INSERT INTO T VALUES (2, 3)");
    execute_statement("INSERT INTO T VALUES (3, 1)");

    run_yield_propagation_test(
        "SELECT R.C0, tvf.c1, tvf.c2 "
        "FROM (SELECT T1.C0 FROM T T1 JOIN T T2 ON T1.C1 = T2.C0) R "
        "CROSS APPLY mock_table_func_blocking(R.C0) AS tvf",
        {"join_scan"},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 100),
            create_nullable_record<kind::int4, kind::int4, kind::int8>(2, 1, 100),
            create_nullable_record<kind::int4, kind::int4, kind::int8>(3, 1, 100),
        }
    );
}

TEST_F(sql_yield_resume_test, join_find) {
    // verify that join_find correctly propagates yield from a downstream apply operator.
    // plan: scan(T1) -> join_find(T2 on T1.C1=T2.C0) -> apply(TVF)
    // joining on T1.C1 = T2.C0 lets the planner look up T2 by primary key (C0) for each
    // T1 row, producing a join_find operator instead of a sort-merge join.
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (1, 2)");
    execute_statement("INSERT INTO T VALUES (2, 1)");

    run_yield_propagation_test(
        "SELECT R.C0, tvf.c1, tvf.c2 "
        "FROM (SELECT T1.C0 FROM T T1 JOIN T T2 ON T1.C1 = T2.C0) R "
        "CROSS APPLY mock_table_func_blocking(R.C0) AS tvf",
        {"join_find"},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 100),
            create_nullable_record<kind::int4, kind::int4, kind::int8>(2, 1, 100),
        }
    );
}

TEST_F(sql_yield_resume_test, take_group_and_flatten) {
    // verify that take_group and flatten correctly propagate yield from a downstream apply operator.
    // plan: scan(T) -> offer(C1) -> group-exchange -> take_group -> flatten -> apply(TVF)
    // GROUP BY C1 with no aggregate function causes the planner to introduce both a take_group
    // and a flatten step operator; both yield-propagation paths are exercised.
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    run_yield_propagation_test(
        "SELECT R.C1, tvf.c1, tvf.c2 "
        "FROM (SELECT C1 FROM T GROUP BY C1) R "
        "CROSS APPLY mock_table_func_blocking(R.C1) AS tvf",
        {"take_group", "flatten"},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int8>(100, 1, 100),
            create_nullable_record<kind::int4, kind::int4, kind::int8>(200, 1, 100),
        }
    );
}

TEST_F(sql_yield_resume_test, take_cogroup_and_join) {
    // verify that take_cogroup and join correctly propagate yield from a downstream apply operator.
    // plan: scan(T1), scan(T2) -> offer(C1) -> cogroup-exchange ->
    //       take_cogroup -> join(T1.C1=T2.C1) -> apply(TVF)
    // the self-join on the non-primary-key column C1 causes the planner to introduce a
    // sort-merge join via take_cogroup and join operators; both yield-propagation paths
    // are exercised.
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    run_yield_propagation_test(
        "SELECT R.C0, tvf.c1, tvf.c2 "
        "FROM (SELECT T1.C0 FROM T T1 JOIN T T2 ON T1.C1 = T2.C1) R "
        "CROSS APPLY mock_table_func_blocking(R.C0) AS tvf",
        {"take_cogroup", "join"},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 100),
            create_nullable_record<kind::int4, kind::int4, kind::int8>(2, 1, 100),
        }
    );
}

TEST_F(sql_yield_resume_test, take_group_and_aggregate_group) {
    // verify that take_group and aggregate_group correctly propagate yield from a downstream
    // apply operator.
    // plan: scan(T) -> offer -> group-exchange -> take_group -> aggregate_group -> apply(TVF)
    // COUNT(DISTINCT C1) with no GROUP BY causes the planner to introduce both a take_group
    // and an aggregate_group step operator; both yield-propagation paths are exercised.
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    run_yield_propagation_test(
        "SELECT R.C, tvf.c1, tvf.c2 "
        "FROM (SELECT CAST(COUNT(DISTINCT C1) AS INT) AS C FROM T) R "
        "CROSS APPLY mock_table_func_blocking(R.C) AS tvf",
        {"take_group", "aggregate_group"},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int8>(2, 1, 100),
        }
    );
}

TEST_F(sql_yield_resume_test, take_flat) {
    // verify that take_flat correctly propagates yield from a downstream apply operator.
    // plan: take_flat(UNION ALL of two scans) -> apply(TVF)
    // UNION ALL causes the planner to introduce a forward relay step where the output
    // is consumed via a take_flat operator, exercising its yield-propagation path.
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    run_yield_propagation_test(
        "SELECT R.C0, tvf.c1, tvf.c2 "
        "FROM (SELECT C0 FROM T WHERE C0 = 1 UNION ALL SELECT C0 FROM T WHERE C0 = 2) R "
        "CROSS APPLY mock_table_func_blocking(R.C0) AS tvf",
        {"take_flat"},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 100),
            create_nullable_record<kind::int4, kind::int4, kind::int8>(2, 1, 100),
        }
    );
}

TEST_F(sql_yield_resume_test, find) {
    // verify that find correctly propagates yield from a downstream apply operator.
    // plan: find(T where C0=1) -> apply(TVF(T.C0))
    // the WHERE T.C0 = 1 condition on the primary key causes the planner to emit a find operator
    // instead of scan, exercising the find operator's yield-propagation path.
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    run_yield_propagation_test(
        "SELECT T.C0, T.C1, tvf.c1, tvf.c2 "
        "FROM T CROSS APPLY mock_table_func_blocking(T.C0) AS tvf "
        "WHERE T.C0 = 1",
        {"find"},
        {
            create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(1, 100, 1, 100),
        }
    );
}

}  // namespace jogasaki::testing
