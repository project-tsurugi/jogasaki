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
#include <jogasaki/meta/field_type_kind.h>
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

/**
 * @brief stream whose blocking behavior is controlled by an atomic flag.
 * @details when blocking_=true the stream returns not_ready until the flag is set,
 *          then delivers one row and ends.  when blocking_=false the stream
 *          delivers the row immediately the first time try_next is called.
 */
class finish_yield_blocking_stream : public data::any_sequence_stream {
public:
    /**
     * @brief constructs the stream.
     * @param flag the flag checked when blocking_=true; ignored when blocking_=false.
     * @param blocking when true the stream blocks until flag is set;
     *                 when false the row is returned immediately.
     * @param v1 int32 value for the single delivered column.
     */
    finish_yield_blocking_stream(
        std::shared_ptr<std::atomic_bool> flag,
        bool blocking,
        std::int32_t v1
    ) noexcept : flag_(std::move(flag)), blocking_(blocking), v1_(v1) {}

    [[nodiscard]] status_type try_next(data::any_sequence& sequence) override {
        if (blocking_ && ! flag_->load()) {
            return status_type::not_ready;
        }
        if (delivered_) {
            return status_type::end_of_stream;
        }
        sequence = data::any_sequence{data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int32_t>, v1_}
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
    bool blocking_{};
    std::int32_t v1_{};
    bool delivered_{false};
};

namespace {

[[nodiscard]] bool contains(std::string_view whole, std::string_view part) noexcept {
    return whole.find(part) != std::string_view::npos;
}

}  // namespace

/**
 * @brief tests that yield occurring inside operator finish() paths is handled correctly.
 * @details reproduces the bug in aggregate_group::finish(): when empty_input_from_shuffle is
 *          true, finish() calls downstream->process_record() to push the empty-aggregate
 *          result to the downstream operator (apply).  If apply yields because the TVF returns
 *          not_ready, aggregate_group::finish() currently ignores the yield status and proceeds
 *          to call downstream->finish() and release the context, causing the apply operator to
 *          be terminated prematurely.  The query therefore returns 0 rows instead of the
 *          expected 1 row.
 *
 *          the non-blocking variant acts as a baseline sanity check: it confirms that the
 *          same query returns the correct result (1 row) when the TVF does not yield, proving
 *          that only the yield path is broken.
 */
class sql_yield_finish_test :
    public ::testing::Test,
    public api_test_base {

public:
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
     * @brief core helper driving both the blocking and non-blocking test cases.
     * @details registers a TVF whose blocking behavior is controlled by blocking_tvf.
     *          when blocking_tvf=true the TVF returns not_ready until unblocked, causing
     *          apply to yield inside aggregate_group::finish().
     *          when blocking_tvf=false the TVF returns its row immediately; no yield
     *          occurs and the correct result (1 row) should always be produced.
     * @param blocking_tvf true to use a blocking TVF (exercises the bug path),
     *                     false to use a non-blocking TVF (baseline sanity check).
     */
    void run_aggregate_group_finish_test(bool blocking_tvf) {
        // create T with no rows: cross join T x T produces 0 rows, making the group exchange
        // empty and triggering the empty_input_from_shuffle path in aggregate_group::finish().
        execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
        // intentionally no INSERT: T remains empty

        auto unblock_flag = std::make_shared<std::atomic_bool>(! blocking_tvf);

        constexpr std::size_t tvf_id = 13002;

        auto mock_func = [unblock_flag, blocking_tvf](
            evaluator_context& /* ctx */,
            sequence_view<data::any> /* args */
        ) -> std::unique_ptr<data::any_sequence_stream> {
            return std::make_unique<finish_yield_blocking_stream>(unblock_flag, blocking_tvf, 1);
        };

        auto decl = global::regular_function_provider()->add(
            std::make_shared<yugawara::function::declaration>(
                tvf_id,
                "mock_table_func_finish_yield",
                std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                    {"c1", std::make_shared<t::int4>()},
                }),
                std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::int8>()},
                yugawara::function::declaration::feature_set_type{
                    yugawara::function::function_feature::table_valued_function
                }
            )
        );

        global::table_valued_function_repository().add(
            tvf_id,
            std::make_shared<table_valued_function_info>(
                table_valued_function_kind::builtin,
                mock_func,
                1,
                table_valued_function_info::columns_type{
                    table_valued_function_column{"c1"}
                }
            )
        );

        // verify the plan contains aggregate_group to confirm the intended code path
        std::string plan{};
        std::string_view query =
            "SELECT R.cnt, tvf.c1 "
            "FROM (SELECT COUNT(DISTINCT C0) AS cnt FROM T) R "
            "CROSS APPLY mock_table_func_finish_yield(R.cnt) AS tvf";
        explain_statement(query, plan);
        ASSERT_TRUE(contains(plan, "aggregate_group"))
            << "expected plan to contain 'aggregate_group' to exercise the finish() bug path, "
            << "but got: " << plan;

        std::vector<mock::basic_record> result{};

        if (blocking_tvf) {
            // create a target table for the concurrent INSERT used to detect yield
            execute_statement("CREATE TABLE T2 (C0 INT PRIMARY KEY)");

            // run the query in background; the single worker enters aggregate_group::finish()
            // → apply->process_record() → TVF returns not_ready → apply yields.
            auto query_future = std::async(std::launch::async, [&]() {
                execute_query(std::string{query}, result);
            });

            // wait for the query to start and the worker to reach the blocking TVF call
            std::this_thread::sleep_for(std::chrono::milliseconds(50));

            // measure INSERT duration: if yield propagated correctly the single worker
            // was freed while the TVF is blocking, so INSERT can run immediately.
            std::atomic<long> insert_duration_ms{-1};
            auto insert_future = std::async(std::launch::async, [&]() {
                auto insert_start = std::chrono::steady_clock::now();
                execute_statement("INSERT INTO T2 VALUES (1)");
                insert_duration_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - insert_start).count());
            });

            // keep the TVF blocked for another 200ms while INSERT runs
            std::this_thread::sleep_for(std::chrono::milliseconds(200));

            // release the TVF so the query can complete
            unblock_flag->store(true);

            insert_future.get();
            query_future.get();

            // with yield propagating correctly: worker freed while TVF blocks → INSERT < 200ms.
            // without yield: worker stuck in finish() → INSERT forced to wait for TVF (~200ms).
            EXPECT_LT(insert_duration_ms.load(), 200);
        } else {
            // non-blocking path: TVF returns immediately, no yield occurs.
            execute_query(std::string{query}, result);
        }

        if (decl) {
            global::regular_function_provider()->remove(*decl);
        }

        // in both cases the expected result is exactly 1 row: (cnt=0, c1=1).
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (create_nullable_record<kind::int8, kind::int4>(std::int64_t{0}, 1)),
            result[0]
        );
    }
};

/**
 * @brief baseline: TVF does not yield.
 * @details the TVF returns its row immediately.  no yield occurs inside
 *          aggregate_group::finish(), so the correct result (1 row) is always produced.
 *          this test must always pass, confirming the non-yield code path is correct.
 */
TEST_F(sql_yield_finish_test, aggregate_group_finish_empty_shuffle_no_yield) {
    run_aggregate_group_finish_test(false);
}

/**
 * @brief regression test: TVF yields inside aggregate_group::finish().
 * @details the TVF initially returns not_ready, causing apply to return yield to
 *          aggregate_group::finish().  the current implementation ignores the yield,
 *          calls apply->finish() prematurely, and the query returns 0 rows.
 *
 *          this test CURRENTLY FAILS because of that bug.  it is intended as a
 *          regression guard: it should pass once the bug is fixed
 *          (see docs/internal/task-yield.md).
 */
TEST_F(sql_yield_finish_test, aggregate_group_finish_empty_shuffle_with_yield) {
    run_aggregate_group_finish_test(true);
}

}  // namespace jogasaki::testing
