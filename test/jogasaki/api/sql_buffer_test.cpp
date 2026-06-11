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
#include <jogasaki/error_code.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;

/**
 * @brief tests for plans that use the `buffer` relational operator.
 *
 * @details The SQL compiler now generates `takatori::relation::buffer` for
 * plans where a single upstream relation is shared by multiple downstream
 * operators (see docs/internal/buffer-operator.md).
 */
class sql_buffer_test :
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

TEST_F(sql_buffer_test, scalar_subquery_values_referencing_outer_column) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t VALUES (1, 10)");

    auto query = "SELECT (VALUES(t.c0)) FROM t";
    EXPECT_TRUE(has_operator(query, "buffer"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[0]);
}

TEST_F(sql_buffer_test, in_subquery_referencing_outer_column) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (2, 3)");
    execute_statement("INSERT INTO t1 VALUES (1, 20), (2, 10)");

    auto query = "SELECT * FROM t0 WHERE c0 IN (SELECT t1.c1 / t0.c1 FROM t1)";
    EXPECT_TRUE(has_operator(query, "buffer"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
}

TEST_F(sql_buffer_test, scalar_subquery_select_list_correlated) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (2, 20)");
    execute_statement("INSERT INTO t1 VALUES (1, 100), (2, 200)");

    auto query = "SELECT t0.c0, (SELECT t1.c1 FROM t1 WHERE t1.c0 = t0.c0) FROM t0 ORDER BY t0.c0";
    EXPECT_TRUE(has_operator(query, "buffer"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 200)), result[1]);
}

TEST_F(sql_buffer_test, exists_subquery_correlated) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10), (2, 20), (3, 30)");
    execute_statement("INSERT INTO t1 VALUES (1, 100), (2, 200)");

    auto query = "SELECT * FROM t0 WHERE EXISTS (SELECT * FROM t1 WHERE t1.c0 = t0.c0) ORDER BY t0.c0";
    EXPECT_TRUE(has_operator(query, "buffer"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 10)), result[0]);
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 20)), result[1]);
}

TEST_F(sql_buffer_test, scalar_subquery_where_comparison_correlated) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 100), (2, 20)");
    execute_statement("INSERT INTO t1 VALUES (1, 100), (2, 200)");

    auto query = "SELECT * FROM t0 WHERE t0.c1 = (SELECT t1.c1 FROM t1 WHERE t1.c0 = t0.c0) ORDER BY t0.c0";
    EXPECT_TRUE(has_operator(query, "buffer"));

    std::vector<mock::basic_record> result{};
    execute_query(query, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
}

}  // namespace jogasaki::testing
