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
#include <string>
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
#include <jogasaki/test_utils/custom_any_sequence_stream.h>
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
 * @brief test that apply_max_polls controls how many times try_next is called before yielding.
 * @details with a single worker thread and OCC transactions, INSERT can only run once the
 *          apply operator yields.  each try_next call sleeps 10ms and returns not_ready;
 *          after (apply_max_polls+1) such calls the apply operator yields and INSERT proceeds.
 *          INSERT duration therefore reflects how long the worker was busy polling:
 *
 *            apply_max_polls=0 → 1 try_next call  (100ms),    INSERT waits ~100ms
 *            apply_max_polls=2 → 3 try_next calls (300ms),    INSERT waits ~300ms
 *            apply_max_polls=4 → 5 try_next calls (500ms),    INSERT waits ~500ms
 */
class sql_apply_max_polls_test :
    public ::testing::Test,
    public api_test_base {

public:
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->thread_pool_size(1);
        db_setup(cfg);
        // OCC transactions allow SELECT and INSERT to run concurrently
        utils::set_global_tx_option(utils::create_tx_option{false, true});
    }

    void TearDown() override {
        global::config_pool()->apply_max_polls(0);  // reset to default
        utils::set_global_tx_option(utils::create_tx_option{});
        global::table_valued_function_repository().clear();
        for (auto const& decl : registered_decls_) {
            global::regular_function_provider()->remove(*decl);
        }
        registered_decls_.clear();
        db_teardown();
    }

    /**
     * @brief registers the timing TVF and measures how long INSERT is blocked.
     * @details each try_next call sleeps 100ms and returns not_ready.  after exactly
     *          (max_polls+1) not_ready calls the apply operator yields; the next call
     *          returns end_of_stream.  INSERT is submitted once the TVF signals that its
     *          first call has begun, guaranteeing it is queued while the single worker
     *          thread is inside the polling loop.
     * @param max_polls value to set for configuration::apply_max_polls
     * @return INSERT duration in milliseconds
     */
    long measure_insert_wait_ms(std::size_t max_polls) {
        global::config_pool()->apply_max_polls(max_polls);

        execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
        execute_statement("INSERT INTO T VALUES (1)");
        execute_statement("CREATE TABLE T2 (C0 INT PRIMARY KEY)");

        constexpr std::size_t tvf_id = 12300;
        int const not_ready_count = static_cast<int>(max_polls) + 1;
        auto tvf_started = std::make_shared<std::atomic_bool>(false);

        auto decl = global::regular_function_provider()->add(
            std::make_shared<yugawara::function::declaration>(
                tvf_id,
                "tvf_max_polls_timing",
                std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                    {"c1", std::make_shared<t::int4>()},
                }),
                std::vector<std::shared_ptr<takatori::type::data const>>{
                    std::make_shared<t::int4>(),
                },
                yugawara::function::declaration::feature_set_type{
                    yugawara::function::function_feature::table_valued_function
                }
            )
        );
        registered_decls_.emplace_back(decl);

        global::table_valued_function_repository().add(
            tvf_id,
            std::make_shared<table_valued_function_info>(
                table_valued_function_kind::builtin,
                [not_ready_count, tvf_started](
                    evaluator_context& /* ctx */,
                    sequence_view<data::any> /* args */
                ) -> std::unique_ptr<data::any_sequence_stream> {
                    return std::make_unique<custom_any_sequence_stream>(
                        [not_ready_count, tvf_started, n = 0](
                            data::any_sequence& /* seq */) mutable
                                -> data::any_sequence_stream::status_type {
                            auto call = n++;
                            if (call == 0) {
                                // signal the test thread: INSERT can now be submitted
                                tvf_started->store(true);
                            }
                            if (call < not_ready_count) {
                                // each not_ready call sleeps 100ms to simulate a slow TVF
                                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                                return data::any_sequence_stream::status_type::not_ready;
                            }
                            return data::any_sequence_stream::status_type::end_of_stream;
                        }
                    );
                },
                1,
                table_valued_function_info::columns_type{
                    table_valued_function_column{"c1"},
                }
            )
        );

        std::vector<mock::basic_record> result{};
        auto query_future = std::async(std::launch::async, [&] {
            execute_query(
                "SELECT R.c1 FROM T CROSS APPLY tvf_max_polls_timing(T.C0) AS R(c1)",
                result
            );
        });

        // wait until the worker has entered the first try_next call and is sleeping
        while (! tvf_started->load()) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }

        // INSERT is submitted while the single worker is sleeping inside try_next.
        // INSERT duration reflects how long the worker stays in the polling loop before yielding.
        std::atomic<long> insert_duration_ms{-1};
        auto insert_future = std::async(std::launch::async, [&] {
            auto t0 = std::chrono::steady_clock::now();
            execute_statement("INSERT INTO T2 VALUES (1)");
            insert_duration_ms.store(
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - t0
                ).count()
            );
        });

        insert_future.get();
        query_future.get();

        // query returns 0 rows (TVF returned end_of_stream before any ok)
        EXPECT_TRUE(result.empty());

        return insert_duration_ms.load();
    }

    std::vector<std::shared_ptr<yugawara::function::declaration>> registered_decls_{};
};

/**
 * @brief apply_max_polls=0: yields immediately after the first not_ready.
 * @details the TVF's first try_next call sleeps 100ms then returns not_ready, causing an
 *          immediate yield (1 try_next call).  INSERT waits only ~100ms before the worker
 *          is released.
 */
TEST_F(sql_apply_max_polls_test, apply_max_polls_0_yields_immediately) {
    // 1 try_next call (100ms), yield → INSERT waits ~100ms
    auto duration = measure_insert_wait_ms(0);
    EXPECT_LT(duration, 200);
}

/**
 * @brief apply_max_polls=2: worker polls 3 times (3×100=300ms) before yielding.
 * @details INSERT cannot run until all 3 try_next calls (300ms total) complete before yielding.
 */
TEST_F(sql_apply_max_polls_test, apply_max_polls_2_delays_insert) {
    // 3 try_next calls (300ms), yield → INSERT waits ~300ms
    auto duration = measure_insert_wait_ms(2);
    EXPECT_GE(duration, 200);
    EXPECT_LT(duration, 1000);
}

/**
 * @brief apply_max_polls=4: worker polls 5 times (5×100=500ms) before yielding.
 * @details INSERT cannot run until all 5 try_next calls (500ms total) complete before yielding.
 */
TEST_F(sql_apply_max_polls_test, apply_max_polls_4_delays_insert_more) {
    // 5 try_next calls (500ms), yield → INSERT waits ~500ms
    auto duration = measure_insert_wait_ms(4);
    EXPECT_GE(duration, 400);
    EXPECT_LT(duration, 2000);
}

}  // namespace jogasaki::testing
