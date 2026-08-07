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
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace jogasaki;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

/**
 * @brief tests on the combination of the columns composing a secondary index
 * @details a secondary index key element refers to either a primary key column or a non-key
 * column of the base table, and the engine resolves where to read the value from for each of them.
 * This test covers the representative combinations of those origins. Variations on the sort
 * direction are covered by secondary_index_desc_test.
 */
class secondary_index_combination_test :
    public ::testing::Test,
    public api_test_base {

public:
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

    bool uses_secondary(std::string_view query, std::string_view index_name);
};

static bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

/**
 * @details the query conditions in this test are designed not to determine the primary key
 * uniquely, so that the planner chooses the secondary index rather than the primary one. Verifying
 * it here ensures the assertions on the query result really exercise the secondary index rather
 * than silently falling back to the primary index.
 */
bool secondary_index_combination_test::uses_secondary(
    std::string_view query,
    std::string_view index_name
) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(
        plan, R"("kind":"index","table":"T","simple_name":")" + std::string{index_name} + R"(")");
}

TEST_F(secondary_index_combination_test, index_on_primary_key_column) {
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, PRIMARY KEY(C0, C1))");
    execute_statement("CREATE INDEX I1 ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100)");
    execute_statement("INSERT INTO T VALUES(2, 20, 200)");
    {
        auto query = "SELECT C2 FROM T WHERE C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        std::vector<basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(200)), result[0]);
    }
}

TEST_F(secondary_index_combination_test, index_on_non_key_column) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I1 ON T (C1)");
    execute_statement("INSERT INTO T VALUES(1, 10)");
    execute_statement("INSERT INTO T VALUES(2, 20)");
    {
        auto query = "SELECT C0 FROM T WHERE C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        std::vector<basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(secondary_index_combination_test, index_mixing_primary_key_and_non_key_columns) {
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, PRIMARY KEY(C0, C1))");
    // the index key elements are a non-key column and a primary key column, so the engine must
    // switch the source record between them
    execute_statement("CREATE INDEX I1 ON T (C2, C1)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100)");
    execute_statement("INSERT INTO T VALUES(2, 20, 200)");
    {
        auto query = "SELECT C0 FROM T WHERE C2 = 200 AND C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        std::vector<basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(secondary_index_combination_test, index_on_composite_primary_key_columns) {
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, PRIMARY KEY(C0, C1))");
    // the index lists the primary key columns in the reverse order of the primary key definition
    execute_statement("CREATE INDEX I1 ON T (C1, C0)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100)");
    execute_statement("INSERT INTO T VALUES(2, 20, 200)");
    {
        auto query = "SELECT C2 FROM T WHERE C1 = 20";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        std::vector<basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(200)), result[0]);
    }
}

TEST_F(secondary_index_combination_test, index_on_composite_primary_key_and_non_key_columns) {
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, PRIMARY KEY(C0, C1))");
    // the index holds the whole primary key plus a non-key column, so it is not covered by the
    // primary key and the query goes through the secondary index
    execute_statement("CREATE INDEX I1 ON T (C2, C1, C0)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100)");
    execute_statement("INSERT INTO T VALUES(2, 20, 200)");
    {
        auto query = "SELECT C0 FROM T WHERE C2 = 200";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        std::vector<basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

TEST_F(secondary_index_combination_test, index_maintained_on_update_and_delete) {
    execute_statement(
        "CREATE TABLE T (C0 INT NOT NULL, C1 INT NOT NULL, C2 INT, PRIMARY KEY(C0, C1))");
    execute_statement("CREATE INDEX I1 ON T (C2)");
    execute_statement("INSERT INTO T VALUES(1, 10, 100)");
    execute_statement("INSERT INTO T VALUES(2, 20, 200)");
    execute_statement("UPDATE T SET C2 = 999 WHERE C0 = 1");
    {
        // the entry for the stale index key must be gone and the new one must be visible
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C2 = 100";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_TRUE(result.empty());
    }
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C2 = 999";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
    execute_statement("DELETE FROM T WHERE C0 = 1");
    {
        std::vector<basic_record> result{};
        auto query = "SELECT C0 FROM T WHERE C2 = 999";
        EXPECT_TRUE(uses_secondary(query, "I1"));
        execute_query(query, result);
        ASSERT_TRUE(result.empty());
    }
    {
        std::vector<basic_record> result{};
        execute_query("SELECT C0 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[0]);
    }
}

}  // namespace jogasaki::testing
