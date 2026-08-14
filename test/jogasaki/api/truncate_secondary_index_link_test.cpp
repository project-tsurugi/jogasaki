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
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/table.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

/**
 * @brief tests for the link between a table and its secondary indexes after TRUNCATE TABLE
 *
 * TRUNCATE TABLE re-creates the storage for the table and re-registers the table/primary index
 * definition into the storage provider. The secondary index definitions kept in the provider
 * still refer to the *old* table object, so the association between the table and its secondary
 * indexes is lost (yugawara::storage::provider::each_table_index() matches by table object
 * identity).
 *
 * As a consequence DROP TABLE performed after TRUNCATE TABLE does not drop the secondary
 * indexes belonging to the table, and their definitions/storages are left behind as garbage.
 * Re-creating an index with the same name then fails.
 *
 * Reported in https://github.com/project-tsurugi/tsurugi-issues/issues/812
 */
class truncate_secondary_index_link_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
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

    /**
     * @brief count the indexes that the provider associates with the given table
     */
    std::size_t table_index_count(std::string_view table_name) {
        auto& provider = *db_impl()->tables();
        auto t = provider.find_table(table_name);
        if(! t) {
            return 0;
        }
        std::size_t count = 0;
        provider.each_table_index(*t, [&](std::string_view, std::shared_ptr<yugawara::storage::index const> const&) {
            ++count;
        });
        return count;
    }

    /**
     * @brief verify that the query is executed via a scan/find on the given secondary index
     */
    bool uses_secondary(std::string_view query, std::string_view index_name) {
        std::string plan{};
        explain_statement(query, plan);
        auto needle = "\""s + std::string{index_name} + "\"";
        return plan.find(needle) != std::string::npos;
    }
};

// ─── the table/secondary index association in the provider ───────────────────

TEST_F(truncate_secondary_index_link_test, table_index_association_kept_after_truncate) {
    // TRUNCATE TABLE must not break the association between the table and its secondary indexes
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    ASSERT_EQ(2, table_index_count("t0"));  // primary + secondary

    execute_statement("TRUNCATE TABLE t0");

    EXPECT_EQ(2, table_index_count("t0"));
}

TEST_F(truncate_secondary_index_link_test, table_index_association_kept_after_truncate_multiple_indices) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT, c2 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("CREATE INDEX i1 ON t0 (c2)");
    ASSERT_EQ(3, table_index_count("t0"));

    execute_statement("TRUNCATE TABLE t0");

    EXPECT_EQ(3, table_index_count("t0"));
}

// ─── DROP TABLE after TRUNCATE TABLE ─────────────────────────────────────────

TEST_F(truncate_secondary_index_link_test, drop_table_removes_secondary_index_after_truncate) {
    // reproducer reported on the issue : TRUNCATE used to break the link to the secondary
    // index, so DROP TABLE left it behind and re-creating it with the same name failed
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("INSERT INTO t0 VALUES (1, 100)");
    execute_statement("INSERT INTO t0 VALUES (2, 100)");

    execute_statement("TRUNCATE TABLE t0");
    execute_statement("DROP TABLE t0");

    // the secondary index definition must be gone together with the table
    EXPECT_TRUE(! db_impl()->tables()->find_index("i0"));

    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");

    execute_statement("INSERT INTO t0 VALUES (1, 100)");
    {
        auto query = "SELECT c0, c1 FROM t0 WHERE c1 = 100"s;
        // the query must be answered via the re-created secondary index, not by a full scan
        ASSERT_TRUE(uses_secondary(query, "i0"));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 100)), result[0]);
    }
}

TEST_F(truncate_secondary_index_link_test, drop_table_removes_storage_entry_of_secondary_index_after_truncate) {
    // same as above, verified on the storage manager entries instead of SQL statements
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");

    execute_statement("TRUNCATE TABLE t0");
    ASSERT_TRUE(global::storage_manager()->find_by_name("i0").has_value());

    execute_statement("DROP TABLE t0");

    EXPECT_TRUE(! global::storage_manager()->find_by_name("t0").has_value());
    EXPECT_TRUE(! global::storage_manager()->find_by_name("i0").has_value());
}

TEST_F(truncate_secondary_index_link_test, drop_table_removes_multiple_secondary_indices_after_truncate) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT, c2 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("CREATE INDEX i1 ON t0 (c2)");

    execute_statement("TRUNCATE TABLE t0");
    execute_statement("DROP TABLE t0");

    EXPECT_TRUE(! db_impl()->tables()->find_index("i0"));
    EXPECT_TRUE(! db_impl()->tables()->find_index("i1"));

    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT, c2 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("CREATE INDEX i1 ON t0 (c2)");
}

// ─── DROP INDEX after TRUNCATE TABLE ─────────────────────────────────────────

TEST_F(truncate_secondary_index_link_test, drop_index_after_truncate) {
    // dropping the index explicitly after TRUNCATE must work and must allow re-creation
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");

    execute_statement("TRUNCATE TABLE t0");

    execute_statement("DROP INDEX i0");
    EXPECT_TRUE(! db_impl()->tables()->find_index("i0"));
    EXPECT_TRUE(! global::storage_manager()->find_by_name("i0").has_value());

    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    {
        auto query = "SELECT c0, c1 FROM t0 WHERE c1 = 10"s;
        ASSERT_TRUE(uses_secondary(query, "i0"));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
    }
    execute_statement("DROP TABLE t0");
}

// ─── an index on another table must not be affected ──────────────────────────

TEST_F(truncate_secondary_index_link_test, other_table_index_not_affected) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("CREATE TABLE t1 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i1 ON t1 (c1)");

    execute_statement("TRUNCATE TABLE t0");
    execute_statement("DROP TABLE t0");

    // t1 and i1 must stay intact and usable
    EXPECT_EQ(2, table_index_count("t1"));
    execute_statement("INSERT INTO t1 VALUES (1, 10)");
    {
        auto query = "SELECT c0, c1 FROM t1 WHERE c1 = 10"s;
        ASSERT_TRUE(uses_secondary(query, "i1"));
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        ASSERT_EQ(1, result.size());
    }
    execute_statement("DROP TABLE t1");
    EXPECT_TRUE(! db_impl()->tables()->find_index("i1"));
}

// ─── after restart ───────────────────────────────────────────────────────────

TEST_F(truncate_secondary_index_link_test, drop_table_after_truncate_and_restart) {
    // the same scenario, but the server is restarted between TRUNCATE and DROP TABLE.
    // after recovery the provider is rebuilt from the metadata, so the association should
    // be restored unless the persisted metadata itself is broken.
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("INSERT INTO t0 VALUES (1, 100)");

    execute_statement("TRUNCATE TABLE t0");

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    EXPECT_EQ(2, table_index_count("t0"));

    execute_statement("DROP TABLE t0");
    EXPECT_TRUE(! db_impl()->tables()->find_index("i0"));

    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("DROP TABLE t0");
}

} // namespace jogasaki::testing
