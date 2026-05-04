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
#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/configuration.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

/**
 * @brief SQL-level tests for TRUNCATE TABLE statement.
 *
 * All tests verify observable behavior through SQL only: row counts,
 * re-insertability, and generated identity values after RESTART IDENTITY
 * or CONTINUE IDENTITY.  Internal sequence metadata is not inspected.
 *
 * Tests cover the scenarios listed in docs/internal/truncate-table.md:
 *   - All rows are deleted by TRUNCATE TABLE.
 *   - INSERT works normally after TRUNCATE TABLE.
 *   - Tables with an implicit surrogate PK (no explicit primary key)
 *     under RESTART IDENTITY and CONTINUE IDENTITY.
 *   - Tables with GENERATED ALWAYS/BY DEFAULT AS IDENTITY columns:
 *     RESTART resets the sequence to its initial value; CONTINUE
 *     preserves the current sequence position.
 *   - Tables with secondary indexes: rows are deleted and the index
 *     remains usable after TRUNCATE TABLE.
 *   - Tables with multiple GENERATED AS IDENTITY columns.
 */
class sql_truncate_table_test :
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
};

// ─── basic row deletion ───────────────────────────────────────────────────────

TEST_F(sql_truncate_table_test, all_rows_deleted) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t VALUES (1, 10)");
    execute_statement("INSERT INTO t VALUES (2, 20)");
    execute_statement("INSERT INTO t VALUES (3, 30)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t", result);
        ASSERT_EQ(3, result.size());
    }
    execute_statement("TRUNCATE TABLE t");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_truncate_table_test, insert_after_truncate) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t VALUES (1, 10)");
    execute_statement("TRUNCATE TABLE t");
    execute_statement("INSERT INTO t VALUES (1, 100)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
    }
}

// ─── tables with implicit surrogate primary key ───────────────────────────────
//
// When a table is created without an explicit primary key, an internal
// auto-increment surrogate PK is assigned. The tests below confirm that
// the sequence backing that surrogate PK is handled correctly by TRUNCATE.

TEST_F(sql_truncate_table_test, implicit_pk_restart_identity) {
    execute_statement("CREATE TABLE t (c0 INT)");
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (2)");
    execute_statement("INSERT INTO t VALUES (3)");
    execute_statement("TRUNCATE TABLE t RESTART IDENTITY");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement("INSERT INTO t VALUES (10)");
    execute_statement("INSERT INTO t VALUES (20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t ORDER BY c0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(20)), result[1]);
    }
}

TEST_F(sql_truncate_table_test, implicit_pk_continue_identity) {
    execute_statement("CREATE TABLE t (c0 INT)");
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (2)");
    execute_statement("INSERT INTO t VALUES (3)");
    execute_statement("TRUNCATE TABLE t CONTINUE IDENTITY");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement("INSERT INTO t VALUES (10)");
    execute_statement("INSERT INTO t VALUES (20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t ORDER BY c0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(10)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(20)), result[1]);
    }
}

// ─── GENERATED ALWAYS AS IDENTITY ────────────────────────────────────────────
//
// After inserting N rows the sequence is at N.
// RESTART IDENTITY resets it to the start value (default: 1).
// CONTINUE IDENTITY (the default) keeps it at N, so the next INSERT gets N+1.

TEST_F(sql_truncate_table_test, generated_always_identity_restart) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("INSERT INTO t (c0) VALUES (3)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t ORDER BY c0", result);
        ASSERT_EQ(3, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(2)), result[1]);
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[2]);
    }
    execute_statement("TRUNCATE TABLE t RESTART IDENTITY");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t WHERE c0 = 10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
}

TEST_F(sql_truncate_table_test, generated_always_identity_continue_default) {
    // TRUNCATE without keyword uses CONTINUE IDENTITY by default.
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("INSERT INTO t (c0) VALUES (3)");
    execute_statement("TRUNCATE TABLE t");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t WHERE c0 = 10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(4)), result[0]);
    }
}

TEST_F(sql_truncate_table_test, generated_always_identity_continue_explicit) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("TRUNCATE TABLE t CONTINUE IDENTITY");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t WHERE c0 = 10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[0]);
    }
}

// ─── GENERATED BY DEFAULT AS IDENTITY ────────────────────────────────────────

TEST_F(sql_truncate_table_test, generated_by_default_identity_restart) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT GENERATED BY DEFAULT AS IDENTITY)");
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("INSERT INTO t (c0) VALUES (3)");
    execute_statement("TRUNCATE TABLE t RESTART IDENTITY");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t WHERE c0 = 10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
    }
}

TEST_F(sql_truncate_table_test, generated_by_default_identity_continue) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT GENERATED BY DEFAULT AS IDENTITY)");
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("INSERT INTO t (c0) VALUES (3)");
    execute_statement("TRUNCATE TABLE t CONTINUE IDENTITY");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t", result);
        ASSERT_EQ(0, result.size());
    }
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t WHERE c0 = 10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(4)), result[0]);
    }
}

// ─── custom start value with RESTART ─────────────────────────────────────────
//
// RESTART IDENTITY must reset to the start value specified at table creation,
// not necessarily to 1.

TEST_F(sql_truncate_table_test, generated_identity_custom_start_restart) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY (START 100))");
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t ORDER BY c0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(100)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4>(101)), result[1]);
    }
    execute_statement("TRUNCATE TABLE t RESTART IDENTITY");
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t WHERE c0 = 10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(100)), result[0]);
    }
}

// ─── secondary index ──────────────────────────────────────────────────────────
//
// TRUNCATE TABLE must also clear the secondary index storage so that queries
// using the index return no rows, and new rows can be inserted and found
// via the index after truncation.

TEST_F(sql_truncate_table_test, secondary_index_rows_deleted) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i ON t (c1)");
    execute_statement("INSERT INTO t VALUES (1, 10)");
    execute_statement("INSERT INTO t VALUES (2, 20)");
    execute_statement("INSERT INTO t VALUES (3, 30)");
    execute_statement("TRUNCATE TABLE t");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t", result);
        ASSERT_EQ(0, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t WHERE c1 = 10", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(sql_truncate_table_test, secondary_index_reusable_after_truncate) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i ON t (c1)");
    execute_statement("INSERT INTO t VALUES (1, 10)");
    execute_statement("TRUNCATE TABLE t");
    execute_statement("INSERT INTO t VALUES (1, 100)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0, c1 FROM t WHERE c1 = 100", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
    }
}

// ─── multiple GENERATED AS IDENTITY columns ───────────────────────────────────

TEST_F(sql_truncate_table_test, multiple_generated_identity_restart) {
    execute_statement("CREATE TABLE t ("
                      "c0 INT, "
                      "c1 INT GENERATED ALWAYS AS IDENTITY, "
                      "c2 INT GENERATED ALWAYS AS IDENTITY)");
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1, c2 FROM t ORDER BY c0", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 1)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 2)), result[1]);
    }
    execute_statement("TRUNCATE TABLE t RESTART IDENTITY");
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1, c2 FROM t WHERE c0 = 10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 1)), result[0]);
    }
}

TEST_F(sql_truncate_table_test, multiple_generated_identity_continue) {
    execute_statement("CREATE TABLE t ("
                      "c0 INT, "
                      "c1 INT GENERATED ALWAYS AS IDENTITY, "
                      "c2 INT GENERATED ALWAYS AS IDENTITY)");
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("TRUNCATE TABLE t CONTINUE IDENTITY");
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1, c2 FROM t WHERE c0 = 10", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(3, 3)), result[0]);
    }
}

} // namespace jogasaki::testing
