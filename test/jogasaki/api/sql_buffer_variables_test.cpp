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

#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;

/**
 * @brief tests for cross basic-block variable access in plans containing the
 * `buffer` relational operator.
 *
 * @details As described in docs/internal/buffer-operator.md, the `buffer`
 * operator splits the operator tree into multiple basic blocks, each with its
 * own variable_table (memory region). An operator below a `buffer` can still
 * reference a variable defined above the `buffer` (in an ancestor block) via
 * the value_info's region_id. These tests verify that such cross-block
 * variable references are read from the correct region.
 */
class sql_buffer_variables_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

    /**
     * @brief check whether the given query's execution plan contains a particular operator name.
     * @param query the SQL query to inspect
     * @param op_name the operator name string to look for (e.g. "buffer")
     * @return true if the plan contains the operator name
     */
    bool has_operator(std::string_view query, std::string_view op_name) {
        std::string plan{};
        explain_statement(query, plan);
        return plan.find(op_name) != std::string::npos;
    }
};

using namespace std::string_view_literals;

/**
 * @brief verify that a `buffer` whose downstream branches (here, two
 * `project` -> `offer` chains) write into the *same* forward exchange
 * executes correctly when the `UNION ALL` yields a single row overall.
 *
 * @details Both `offer` operators below `buffer` target the same forward
 * exchange `F` (the `UNION ALL` merges both arms into one exchange). Each
 * `offer` operator is assigned its own dedicated output index by
 * `enumerate_offers()` (src/jogasaki/plan/compiler.cpp), which walks the
 * process's internal operator tree and registers one `add_output()` per
 * `offer` node found - even when multiple `offer` nodes reference the same
 * exchange. `relation_io_map` (src/jogasaki/executor/process/relation_io_map.h)
 * looks up the output index by the `offer` node itself rather than by its
 * destination relation, so the two `offer` operators map to distinct output
 * indices (0 and 1).
 *
 * Because `io_exchange_map::output_count() == 2` for this process,
 * `process::flow::create_tasks()` calls `forward::flow::setup_partitions()`
 * twice for exchange `F`, which (since `setup_partitions()` appends new
 * sinks/sources rather than being idempotent) allocates `2 * partitions`
 * sinks. Each `offer` operator therefore acquires and releases the writer of
 * its own dedicated `forward::sink` exactly once - no sink's writer is ever
 * acquired or released twice.
 *
 * For this query, `tv.c1 = -1`, so the first arm (`SELECT c1 + 1 FROM
 * (VALUES(0)) AS d(x) WHERE c1 > 0`) is filtered out and contributes no row,
 * while the second arm (`SELECT c2 + 2`) always contributes one row. The
 * `UNION ALL` therefore yields exactly one row (`2 + 2 = 4`), which is a
 * valid result for a scalar subquery.
 */
TEST_F(sql_buffer_variables_test, union_all_subquery_with_project_downstream_of_buffer_yields_single_row) {
    auto query = "SELECT (SELECT c1 + 1 FROM (VALUES(0)) AS d(x) WHERE c1 > 0 UNION ALL SELECT c2 + 2) FROM (VALUES (-1, 2)) AS tv(c1, c2)";
    EXPECT_TRUE(has_operator(query, "buffer"));
    EXPECT_TRUE(has_operator(query, "project"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int8>(4)), result[0]);
}

/**
 * @brief same as
 * `union_all_subquery_with_project_downstream_of_buffer_yields_single_row`,
 * but with one of `buffer`'s downstream branches being `filter -> offer`
 * instead of `project -> offer`. This shows that the per-`offer` output
 * index assignment in `enumerate_offers()` is independent of the specific
 * operator that sits between `buffer` and `offer`: it applies whenever
 * multiple of `buffer`'s downstream branches write to the same forward
 * exchange.
 *
 * @details For this query, `tv.c1 = -1`, so the first arm (`SELECT c1 FROM
 * (VALUES(0)) AS d(x) WHERE c1 > 0`) is filtered out by the `filter` operator
 * below `buffer` and contributes no row, while the second arm (`SELECT c2`)
 * always contributes one row. The `UNION ALL` therefore yields exactly one
 * row (`c2 = 2`), which is a valid result for a scalar subquery.
 */
TEST_F(sql_buffer_variables_test, union_all_subquery_with_filter_downstream_of_buffer_yields_single_row) {
    auto query = "SELECT (SELECT c1 FROM (VALUES(0)) AS d(x) WHERE c1 > 0 UNION ALL SELECT c2) FROM (VALUES (-1, 2)) AS tv(c1, c2)";
    EXPECT_TRUE(has_operator(query, "buffer"));
    EXPECT_TRUE(has_operator(query, "filter"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int8>(2)), result[0]);
}

}  // namespace jogasaki::testing
