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
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/maintenance_storage.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

/**
 * @brief tests for TRUNCATE TABLE executed by a transaction that finally fails
 *
 * TRUNCATE TABLE creates brand-new storages for the target table and its secondary indexes,
 * and reserves the deletion of the old ones. Creating/deleting storages is not part of the
 * transaction, so TRUNCATE TABLE is not transactional : its effect is kept even when the
 * transaction that issued it finally fails. That is the specified behavior.
 *
 * What must not happen is that the failure leaves the database in a broken or inconsistent
 * state, e.g. dangling sequence metadata, storages left behind as garbage, or a table that
 * cannot be used, restarted or dropped afterwards. The tests below verify that.
 *
 * Reported in https://github.com/project-tsurugi/tsurugi-issues/issues/812
 */
class truncate_transaction_fail_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_truncate(true);
        cfg->enable_maintenance_thread(false);  // run maintenance explicitly in tests
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

    std::size_t count_rows(std::string_view query) {
        std::vector<mock::basic_record> result{};
        execute_query(query, result);
        return result.size();
    }

    std::size_t delete_reserved_count() {
        return global::storage_manager()->get_delete_reserved_entries().size();
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

// ─── TRUNCATE is not transactional : the effect is kept on failure ───────────

TEST_F(truncate_transaction_fail_test, aborted_truncate_takes_effect) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t0", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    EXPECT_EQ(0, count_rows("SELECT c0, c1 FROM t0"));
    // the table stays usable
    execute_statement("INSERT INTO t0 VALUES (3, 30)");
    EXPECT_EQ(1, count_rows("SELECT c0, c1 FROM t0"));
}

TEST_F(truncate_transaction_fail_test, aborted_truncate_keeps_secondary_index_consistent) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t0", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    // the secondary index is truncated together with the table and stays consistent with it
    EXPECT_EQ(0, count_rows("SELECT c0, c1 FROM t0"));
    {
        auto query = "SELECT c0, c1 FROM t0 WHERE c1 = 10"s;
        ASSERT_TRUE(uses_secondary(query, "i0"));
        EXPECT_EQ(0, count_rows(query));
    }
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    {
        auto query = "SELECT c0, c1 FROM t0 WHERE c1 = 10"s;
        ASSERT_TRUE(uses_secondary(query, "i0"));
        EXPECT_EQ(1, count_rows(query));
    }
}

// ─── no storage is left behind by an aborted TRUNCATE ────────────────────────

TEST_F(truncate_transaction_fail_test, aborted_truncate_leaves_no_garbage_storage) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    auto entries_before = global::storage_manager()->size();
    ASSERT_EQ(0, delete_reserved_count());
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t0", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    // the storages replaced by TRUNCATE are reserved for deletion and removed by maintenance,
    // so no storage is left behind
    EXPECT_EQ(2, storage::maintenance_storage().size());
    EXPECT_EQ(entries_before, global::storage_manager()->size());
    EXPECT_EQ(0, delete_reserved_count());
}

TEST_F(truncate_transaction_fail_test, aborted_truncate_usable_after_maintenance) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t0", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    storage::maintenance_storage();
    EXPECT_EQ(0, count_rows("SELECT c0, c1 FROM t0"));
    execute_statement("INSERT INTO t0 VALUES (3, 30)");
    EXPECT_EQ(1, count_rows("SELECT c0, c1 FROM t0 WHERE c1 = 30"));
}

TEST_F(truncate_transaction_fail_test, aborted_truncate_consistent_after_restart) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t0", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    storage::maintenance_storage();
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    EXPECT_EQ(0, count_rows("SELECT c0, c1 FROM t0"));
    execute_statement("INSERT INTO t0 VALUES (3, 30)");
    {
        auto query = "SELECT c0, c1 FROM t0 WHERE c1 = 30"s;
        ASSERT_TRUE(uses_secondary(query, "i0"));
        EXPECT_EQ(1, count_rows(query));
    }
    execute_statement("DROP TABLE t0");
    // the secondary index has been dropped together with the table
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("DROP TABLE t0");
}

// ─── identity sequences must stay consistent after an aborted TRUNCATE ───────

TEST_F(truncate_transaction_fail_test, aborted_truncate_restart_identity_keeps_sequence_usable) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    execute_statement("INSERT INTO t0 (c0) VALUES (1)");
    execute_statement("INSERT INTO t0 (c0) VALUES (2)");
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t0", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    // the sequence has been restarted together with the table
    execute_statement("INSERT INTO t0 (c0) VALUES (3)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t0 WHERE c0 = 3", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[0]);
    }
    // the sequence metadata must not be broken by the failure : the sequence keeps working
    // after a restart and generates values without duplicating the existing ones
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    execute_statement("INSERT INTO t0 (c0) VALUES (4)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t0 WHERE c0 = 4", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4>(4)), result[0]);
    }
    execute_statement("DROP TABLE t0");
}

// ─── the table must stay usable and truncatable after the failure ────────────

TEST_F(truncate_transaction_fail_test, truncate_retriable_after_abort) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t0", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    // retry and commit this time
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t0", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
    EXPECT_EQ(0, count_rows("SELECT c0, c1 FROM t0"));
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    {
        auto query = "SELECT c0, c1 FROM t0 WHERE c1 = 20"s;
        ASSERT_TRUE(uses_secondary(query, "i0"));
        EXPECT_EQ(1, count_rows(query));
    }
    execute_statement("DROP TABLE t0");
}

// ─── sanity check : committed TRUNCATE behaves normally ──────────────────────

TEST_F(truncate_transaction_fail_test, committed_truncate_deletes_rows) {
    execute_statement("CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX i0 ON t0 (c1)");
    execute_statement("INSERT INTO t0 VALUES (1, 10)");
    execute_statement("INSERT INTO t0 VALUES (2, 20)");
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t0", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
    EXPECT_EQ(0, count_rows("SELECT c0, c1 FROM t0"));
    {
        auto query = "SELECT c0, c1 FROM t0 WHERE c1 = 10"s;
        ASSERT_TRUE(uses_secondary(query, "i0"));
        EXPECT_EQ(0, count_rows(query));
    }
}

} // namespace jogasaki::testing
