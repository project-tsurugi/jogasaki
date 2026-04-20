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

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/maintenance_storage.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

/**
 * @brief Regression tests for tsurugi-issues #177: lazy storage deletion.
 *
 * These tests reproduce crashes that occur when shirakami's delete_storage is called
 * while a transaction still internally references the storage.
 *
 * All tests are skipped on the memory sharksfin implementation because the crash
 * only manifests with shirakami.
 *
 * Expected behavior BEFORE the fix: the test process crashes (SIGSEGV) at the
 * annotated line. Expected behavior AFTER the fix: all operations complete without
 * crash, because storage deletion is deferred until the reference count reaches 0.
 */

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

class sql_lazy_delete_storage_test :
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

// ─── Test 1-1 ────────────────────────────────────────────────────────────────
// Two transactions: DML tx queries a table; DDL tx drops that table before the
// DML tx commits.
//
// Sequence:
//   tx_dml: BEGIN
//   tx_dml: SELECT * FROM t       ← shared lock acquired and released after stmt
//   (auto-tx): DROP TABLE t       ← write lock acquired; delete_storage called
//                                    immediately (BUG); write lock released on commit
//   tx_dml: COMMIT                ← crash: shirakami still references deleted storage
//
// After fix: DROP TABLE reserves deletion; tx_dml commit decrements ref count to 0;
// maintenance thread calls delete_storage safely.
TEST_F(sql_lazy_delete_storage_test, concurrent_dml_then_drop_table_then_dml_commit) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "crash occurs only with shirakami implementation";
    }
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (2)");

    // DML tx (OCC): the SELECT completes and releases its shared lock, but the tx
    // remains open — shirakami still references the storage internally.
    auto tx_dml = utils::create_transaction(*db_, false, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx_dml, result);
    ASSERT_EQ(2, result.size());

    // The shared lock from tx_dml's SELECT was already released, so the DDL auto-tx
    // can acquire the write lock and execute DROP TABLE.
    // BUG (before fix): delete_storage is called immediately while shirakami still
    // has an internal reference from tx_dml.
    execute_statement("DROP TABLE t");

    // CRASH HERE (before fix): shirakami accesses the already-deleted storage.
    ASSERT_EQ(status::ok, tx_dml->commit());
}

// ─── Test 1-2 ────────────────────────────────────────────────────────────────
// Two transactions: DML tx queries a table using a secondary index; DDL tx drops
// that index before the DML tx commits.
//
// Sequence:
//   tx_dml: BEGIN
//   tx_dml: SELECT * FROM t WHERE c1 = 10   ← uses secondary index idx0; shared lock released
//   (auto-tx): DROP INDEX idx0               ← delete_storage on index idx0 called immediately (BUG)
//   tx_dml: COMMIT                           ← crash: shirakami still references deleted index
//
// After fix: index storage deletion is deferred; tx_dml commit is safe.
TEST_F(sql_lazy_delete_storage_test, concurrent_dml_using_index_then_drop_index_then_dml_commit) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "crash occurs only with shirakami implementation";
    }
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");
    execute_statement("INSERT INTO t VALUES (1, 10)");
    execute_statement("INSERT INTO t VALUES (2, 20)");

    // verify that the SELECT uses idx0 via the secondary index find path
    {
        std::string plan{};
        explain_statement("SELECT * FROM t WHERE c1 = 10", plan);
        EXPECT_TRUE(contains(plan, R"("simple_name":"idx0")"));
    }

    // DML tx (OCC): SELECT uses secondary index idx0; shared lock released after statement.
    auto tx_dml = utils::create_transaction(*db_, false, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t WHERE c1 = 10", *tx_dml, result);
    ASSERT_EQ(1, result.size());

    // The shared lock is released, so the DDL auto-tx can drop the index.
    // BUG (before fix): delete_storage on the index storage is called immediately.
    execute_statement("DROP INDEX idx0");

    // CRASH HERE (before fix): shirakami accesses the already-deleted index storage.
    ASSERT_EQ(status::ok, tx_dml->commit());
}

// ─── Test 2-1 ────────────────────────────────────────────────────────────────
// Single transaction: CREATE TABLE → INSERT → SELECT → DROP TABLE → COMMIT.
//
// After SELECT, shirakami internally references the storage.  DROP TABLE in the
// same transaction calls delete_storage immediately while the storage is still
// referenced.
//
// CRASH HERE (before fix): at the DROP TABLE statement.
// After fix: DROP TABLE reserves deletion; COMMIT decrements ref count to 0;
// maintenance thread calls delete_storage safely after commit.
TEST_F(sql_lazy_delete_storage_test, single_tx_create_insert_select_drop_table_commit) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "crash occurs only with shirakami implementation";
    }
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use OCC
    auto tx = utils::create_transaction(*db_);
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)", *tx);
    execute_statement("INSERT INTO t VALUES (1)", *tx);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    ASSERT_EQ(1, result.size());
    // CRASH HERE (before fix): the SELECT caused shirakami to reference the storage;
    // DROP TABLE calls delete_storage immediately while the reference still exists.
    execute_statement("DROP TABLE t", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

// ─── Test 2-2 ────────────────────────────────────────────────────────────────
// Single transaction: CREATE TABLE → CREATE INDEX → INSERT → SELECT (via index)
// → DROP TABLE → COMMIT.
//
// After SELECT (which uses the secondary index), shirakami internally references
// both the primary and the secondary index storage.  DROP TABLE calls
// delete_storage on both storages immediately while they are still referenced.
//
// CRASH HERE (before fix): at the DROP TABLE statement.
// After fix: deletion is deferred for both storages; COMMIT is safe.
TEST_F(sql_lazy_delete_storage_test, single_tx_create_insert_select_via_index_drop_table_commit) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "crash occurs only with shirakami implementation";
    }
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use OCC
    auto tx = utils::create_transaction(*db_);
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)", *tx);
    execute_statement("CREATE INDEX idx0 ON t (c1)", *tx);
    execute_statement("INSERT INTO t VALUES (1, 10)", *tx);

    // verify that the SELECT uses idx0 via the secondary index find path
    {
        std::string plan{};
        explain_statement("SELECT * FROM t WHERE c1 = 10", plan);
        EXPECT_TRUE(contains(plan, R"("simple_name":"idx0")"));
    }

    std::vector<mock::basic_record> result{};
    // SELECT uses secondary index idx0 to look up rows by c1.
    execute_query("SELECT * FROM t WHERE c1 = 10", *tx, result);
    ASSERT_EQ(1, result.size());
    // CRASH HERE (before fix): shirakami still references both the primary and
    // the secondary index storage after the SELECT.
    execute_statement("DROP TABLE t", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

} // namespace jogasaki::testing
