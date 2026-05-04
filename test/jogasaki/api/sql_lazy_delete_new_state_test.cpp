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
#include <cstddef>
#include <memory>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/maintenance_storage.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::executor;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

// verify the new state of storage_manager entry after DROP TABLE/INDEX and before actual deletion by maintenance_storage()
class sql_lazy_delete_new_state_test :
    public ::testing::Test,
    public api_test_base {

public:
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_maintenance_thread(false);   // manual maintenance in tests
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

// After DROP TABLE the storage_manager should still hold the entry (delete_reserved)
// and the name should be invisible to find_by_name().
TEST_F(sql_lazy_delete_new_state_test, drop_table_marks_delete_reserved) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");

    auto& smgr = *global::storage_manager();
    auto entry = smgr.find_by_name("t");
    ASSERT_TRUE(entry.has_value());
    auto ctrl_before = smgr.find_entry(entry.value());
    ASSERT_TRUE(ctrl_before != nullptr);
    EXPECT_TRUE(! ctrl_before->delete_reserved());

    execute_statement("DROP TABLE t");

    // name should be gone
    EXPECT_TRUE(! smgr.find_by_name("t").has_value());
    // but the entry itself must still exist and be flagged
    auto ctrl_after = smgr.find_entry(entry.value());
    ASSERT_TRUE(ctrl_after != nullptr);
    EXPECT_TRUE(ctrl_after->delete_reserved());
}

// After DROP INDEX the storage_manager entry for the secondary is flagged.
TEST_F(sql_lazy_delete_new_state_test, drop_index_marks_delete_reserved) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");

    auto& smgr = *global::storage_manager();
    auto idx_entry = smgr.find_by_name("idx0");
    ASSERT_TRUE(idx_entry.has_value());

    execute_statement("DROP INDEX idx0");

    // secondary name should be invisible
    EXPECT_TRUE(! smgr.find_by_name("idx0").has_value());
    // but the entry exists and is flagged
    auto ctrl = smgr.find_entry(idx_entry.value());
    ASSERT_TRUE(ctrl != nullptr);
    EXPECT_TRUE(ctrl->delete_reserved());
}

// maintenance_storage() should delete a delete-reserved storage when ref count is 0.
TEST_F(sql_lazy_delete_new_state_test, maintenance_storage_deletes_reserved_storage) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");

    auto& smgr = *global::storage_manager();
    auto entry = smgr.find_by_name("t");
    ASSERT_TRUE(entry.has_value());
    auto e = entry.value();

    execute_statement("DROP TABLE t");

    // ref count is 0 (no open tx references it), so maintenance should delete it
    ASSERT_EQ((std::vector<std::string>{"t"}), storage::maintenance_storage());

    // entry should be completely gone now
    EXPECT_TRUE(smgr.find_entry(e) == nullptr);
}

// maintenance_storage() should NOT delete a delete-reserved storage when ref count > 0.
TEST_F(sql_lazy_delete_new_state_test, maintenance_storage_keeps_reserved_storage_when_referenced) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support lazy delete with ref count";  // TODO check
    }
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (1)");

    auto& smgr = *global::storage_manager();
    auto entry = smgr.find_by_name("t");
    ASSERT_TRUE(entry.has_value());
    auto e = entry.value();

    // Open a DML tx that references 't' (increments ref count)
    auto tx_dml = utils::create_transaction(*db_, false, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx_dml, result);
    ASSERT_EQ(1, result.size());

    // Drop the table while the DML tx is still open
    execute_statement("DROP TABLE t");

    // ref count is still > 0 — maintenance must not delete yet
    EXPECT_TRUE(storage::maintenance_storage().empty());

    // Commit the DML tx — ref count drops to 0
    ASSERT_EQ(status::ok, tx_dml->commit());

    // Now maintenance can clean it up
    ASSERT_EQ((std::vector<std::string>{"t"}), storage::maintenance_storage());
    EXPECT_TRUE(! smgr.find_entry(e));
}

// When DROP INDEX is executed, a TX that queries via the secondary index keeps the index storage alive.
TEST_F(sql_lazy_delete_new_state_test, maintenance_storage_keeps_index_storage_when_index_tx_open) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support lazy delete with ref count";  // TODO check
    }
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");
    execute_statement("INSERT INTO t VALUES (1, 10)");

    // verify that the SELECT uses idx0 via the secondary index find path
    {
        std::string plan{};
        explain_statement("SELECT * FROM t WHERE c1 = 10", plan);
        EXPECT_TRUE(plan.find(R"("simple_name":"idx0")") != std::string::npos);
    }

    auto& smgr = *global::storage_manager();
    auto idx_entry = smgr.find_by_name("idx0");
    ASSERT_TRUE(idx_entry.has_value());
    auto e = idx_entry.value();

    // Open a DML tx that uses the secondary index (query on indexed column)
    auto tx_dml = utils::create_transaction(*db_, false, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t WHERE c1 = 10", *tx_dml, result);
    ASSERT_EQ(1, result.size());

    // Drop the index while the DML tx is still open
    execute_statement("DROP INDEX idx0");

    // ref count is still > 0 — maintenance must not delete yet
    EXPECT_TRUE(storage::maintenance_storage().empty());

    // Commit the DML tx — ref count drops to 0
    ASSERT_EQ(status::ok, tx_dml->commit());

    // Now maintenance can clean it up
    ASSERT_EQ((std::vector<std::string>{"idx0"}), storage::maintenance_storage());
    EXPECT_TRUE(! smgr.find_entry(e));
}

// When DROP INDEX is executed, a TX that queries the base table keeps the index storage alive.
TEST_F(sql_lazy_delete_new_state_test, maintenance_storage_keeps_index_storage_when_base_table_tx_open) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support lazy delete with ref count";  // TODO check
    }
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");
    execute_statement("INSERT INTO t VALUES (1, 10)");

    auto& smgr = *global::storage_manager();
    auto idx_entry = smgr.find_by_name("idx0");
    ASSERT_TRUE(idx_entry.has_value());
    auto e = idx_entry.value();

    // Open a DML tx that scans the base table (primary storage)
    auto tx_dml = utils::create_transaction(*db_, false, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx_dml, result);
    ASSERT_EQ(1, result.size());

    // Drop the index while the DML tx is still open
    execute_statement("DROP INDEX idx0");

    // ref count is still > 0 — maintenance must not delete yet
    EXPECT_TRUE(storage::maintenance_storage().empty());

    // Commit the DML tx — ref count drops to 0
    ASSERT_EQ(status::ok, tx_dml->commit());

    // Now maintenance can clean it up
    ASSERT_EQ((std::vector<std::string>{"idx0"}), storage::maintenance_storage());
    EXPECT_TRUE(! smgr.find_entry(e));
}

// When a TX referencing an unrelated table/index is open, DROP TABLE on a different
// table still succeeds and maintenance_storage() deletes the dropped table's storages.
TEST_F(sql_lazy_delete_new_state_test, maintenance_storage_deletes_storage_with_unrelated_tx_open) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support lazy delete with ref count";
    }
    execute_statement("CREATE TABLE other (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX other_idx ON other (c1)");
    execute_statement("INSERT INTO other VALUES (1, 10)");

    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");
    execute_statement("INSERT INTO t VALUES (2, 20)");

    auto& smgr = *global::storage_manager();
    auto t_entry = smgr.find_by_name("t");
    ASSERT_TRUE(t_entry.has_value());
    auto t_e = t_entry.value();
    auto idx_entry = smgr.find_by_name("idx0");
    ASSERT_TRUE(idx_entry.has_value());
    auto idx_e = idx_entry.value();

    // Open a DML tx that only touches the unrelated table/index
    auto tx_unrelated = utils::create_transaction(*db_, false, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM other WHERE c1 = 10", *tx_unrelated, result);
    ASSERT_EQ(1, result.size());

    // Drop the unrelated table while tx_unrelated is open — ref count on 'other' > 0
    // but 't' and 'idx0' have ref count 0, so they should be cleaned up by maintenance
    execute_statement("DROP TABLE t");

    // 't' and 'idx0' ref counts are 0 — maintenance must delete them
    auto deleted = storage::maintenance_storage();
    std::sort(deleted.begin(), deleted.end());
    ASSERT_EQ((std::vector<std::string>{"idx0", "t"}), deleted);
    EXPECT_TRUE(! smgr.find_entry(t_e));
    EXPECT_TRUE(! smgr.find_entry(idx_e));

    // Commit the unrelated tx — 'other' and 'other_idx' are still intact
    ASSERT_EQ(status::ok, tx_unrelated->commit());
    EXPECT_TRUE(smgr.find_by_name("other").has_value());
    EXPECT_TRUE(smgr.find_by_name("other_idx").has_value());
}


// Return the number of entries in the sequence system table.
static std::size_t count_sequences(api::database& db) {
    auto tx = utils::create_transaction(db);
    auto tctx = get_transaction_context(*tx);
    executor::sequence::metadata_store ms{*tctx->object()};
    std::size_t count = 0;
    ms.scan([&count](std::size_t, std::size_t) { ++count; });
    return count;
}

// After DROP TABLE on a rowid table (no explicit PK), the generated rowid sequence
// must be removed from configuration provider and sequence manager immediately so
// that CREATE TABLE with the same name succeeds before maintenance_storage() runs.
TEST_F(sql_lazy_delete_new_state_test, recreate_rowid_table_before_maintenance) {
    auto seq_count_before = count_sequences(*db_);

    // no explicit PK → one rowid sequence generated
    execute_statement("CREATE TABLE t (c0 INT)");
    EXPECT_EQ(seq_count_before + 1, count_sequences(*db_));

    // Insert multiple rows before DROP.
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("INSERT INTO t (c0) VALUES (3)");

    auto& smgr = *global::storage_manager();
    auto entry = smgr.find_by_name("t");
    ASSERT_TRUE(entry.has_value());
    auto old_e = entry.value();

    execute_statement("DROP TABLE t");

    // Rowid sequence must be removed immediately (before maintenance).
    EXPECT_EQ(seq_count_before, count_sequences(*db_));
    // Storage must remain with delete_reserved flag.
    auto ctrl = smgr.find_entry(old_e);
    ASSERT_TRUE(ctrl != nullptr);
    EXPECT_TRUE(ctrl->delete_reserved());

    // Recreate the same-named table before calling maintenance_storage().
    execute_statement("CREATE TABLE t (c0 INT)");
    EXPECT_EQ(seq_count_before + 1, count_sequences(*db_));

    // DML on the new table must work without conflict.
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    execute_statement("INSERT INTO t (c0) VALUES (20)");
    execute_statement("INSERT INTO t (c0) VALUES (30)");

    // maintenance_storage() must delete exactly the one old reserved storage.
    ASSERT_EQ((std::vector<std::string>{"t"}), storage::maintenance_storage());
    EXPECT_TRUE(smgr.find_entry(old_e) == nullptr);
}

// After DROP TABLE on a table with a GENERATED ALWAYS AS IDENTITY column, the
// generated sequence must be removed from configuration provider and sequence manager
// immediately so that CREATE TABLE with the same name succeeds before
// maintenance_storage() runs.
TEST_F(sql_lazy_delete_new_state_test, recreate_identity_table_before_maintenance) {
    auto seq_count_before = count_sequences(*db_);

    // c1 is GENERATED ALWAYS AS IDENTITY → one identity sequence generated
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    EXPECT_EQ(seq_count_before + 1, count_sequences(*db_));

    // Insert multiple rows before DROP.
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("INSERT INTO t (c0) VALUES (3)");

    auto& smgr = *global::storage_manager();
    auto entry = smgr.find_by_name("t");
    ASSERT_TRUE(entry.has_value());
    auto old_e = entry.value();

    execute_statement("DROP TABLE t");

    // Identity sequence must be removed immediately (before maintenance).
    EXPECT_EQ(seq_count_before, count_sequences(*db_));
    // Storage must remain with delete_reserved flag.
    auto ctrl = smgr.find_entry(old_e);
    ASSERT_TRUE(ctrl != nullptr);
    EXPECT_TRUE(ctrl->delete_reserved());

    // Recreate the same-named table before calling maintenance_storage().
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    EXPECT_EQ(seq_count_before + 1, count_sequences(*db_));

    // DML on the new table must work without conflict.
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    execute_statement("INSERT INTO t (c0) VALUES (20)");
    execute_statement("INSERT INTO t (c0) VALUES (30)");

    // maintenance_storage() must delete exactly the one old reserved storage.
    ASSERT_EQ((std::vector<std::string>{"t"}), storage::maintenance_storage());
    EXPECT_TRUE(smgr.find_entry(old_e) == nullptr);
}

// ─── Tests: TRUNCATE TABLE ────────────────────────────────────────────────────

// After TRUNCATE TABLE the old primary storage entry is flagged delete_reserved,
// while the table name remains visible in storage_manager (pointing to a new entry)
// and the table metadata stays in the configuration provider.
TEST_F(sql_lazy_delete_new_state_test, truncate_table_marks_old_primary_delete_reserved) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (1)");

    auto& smgr = *global::storage_manager();
    auto old_entry = smgr.find_by_name("t");
    ASSERT_TRUE(old_entry.has_value());
    auto old_e = old_entry.value();

    auto ctrl_before = smgr.find_entry(old_e);
    ASSERT_TRUE(ctrl_before != nullptr);
    EXPECT_TRUE(! ctrl_before->delete_reserved());

    execute_statement("TRUNCATE TABLE t");

    // table name must still be visible in storage_manager (new entry)
    auto new_entry_opt = smgr.find_by_name("t");
    ASSERT_TRUE(new_entry_opt.has_value());
    auto new_e = new_entry_opt.value();
    EXPECT_TRUE(new_e != old_e);

    // old entry must be delete_reserved and invisible by name
    auto old_ctrl = smgr.find_entry(old_e);
    ASSERT_TRUE(old_ctrl != nullptr);
    EXPECT_TRUE(old_ctrl->delete_reserved());

    // new entry must be active (not delete_reserved)
    auto new_ctrl = smgr.find_entry(new_e);
    ASSERT_TRUE(new_ctrl != nullptr);
    EXPECT_TRUE(! new_ctrl->delete_reserved());

    // table metadata must still be in provider
    EXPECT_TRUE(db_impl()->tables()->find_table("t"));

    // table must be usable after TRUNCATE
    execute_statement("INSERT INTO t VALUES (2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", result);
    ASSERT_EQ(1, result.size());
}

// After TRUNCATE TABLE on a table with a secondary index, both old storages are
// delete_reserved while new active ones replace them.
TEST_F(sql_lazy_delete_new_state_test, truncate_table_with_secondary_marks_old_storages_delete_reserved) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");
    execute_statement("INSERT INTO t VALUES (1, 10)");

    auto& smgr = *global::storage_manager();
    auto old_t = smgr.find_by_name("t").value();
    auto old_idx = smgr.find_by_name("idx0").value();

    execute_statement("TRUNCATE TABLE t");

    // both names must still be visible (new entries created)
    auto new_t_opt = smgr.find_by_name("t");
    auto new_idx_opt = smgr.find_by_name("idx0");
    ASSERT_TRUE(new_t_opt.has_value());
    ASSERT_TRUE(new_idx_opt.has_value());
    auto new_t = new_t_opt.value();
    auto new_idx = new_idx_opt.value();
    EXPECT_TRUE(new_t != old_t);
    EXPECT_TRUE(new_idx != old_idx);

    // old entries must be delete_reserved
    auto old_t_ctrl = smgr.find_entry(old_t);
    ASSERT_TRUE(old_t_ctrl != nullptr);
    EXPECT_TRUE(old_t_ctrl->delete_reserved());

    auto old_idx_ctrl = smgr.find_entry(old_idx);
    ASSERT_TRUE(old_idx_ctrl != nullptr);
    EXPECT_TRUE(old_idx_ctrl->delete_reserved());

    // new entries must be active
    auto new_t_ctrl = smgr.find_entry(new_t);
    ASSERT_TRUE(new_t_ctrl != nullptr);
    EXPECT_TRUE(! new_t_ctrl->delete_reserved());

    auto new_idx_ctrl = smgr.find_entry(new_idx);
    ASSERT_TRUE(new_idx_ctrl != nullptr);
    EXPECT_TRUE(! new_idx_ctrl->delete_reserved());
}

// maintenance_storage() deletes the old storages created by TRUNCATE while
// the new active storages remain intact.
TEST_F(sql_lazy_delete_new_state_test, maintenance_storage_deletes_truncated_old_storages) {
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");

    auto& smgr = *global::storage_manager();
    auto old_t = smgr.find_by_name("t").value();
    auto old_idx = smgr.find_by_name("idx0").value();

    execute_statement("TRUNCATE TABLE t");

    auto new_t = smgr.find_by_name("t").value();
    auto new_idx = smgr.find_by_name("idx0").value();

    // maintenance must delete only the two old delete-reserved storages
    auto deleted = storage::maintenance_storage();
    std::sort(deleted.begin(), deleted.end());
    ASSERT_EQ((std::vector<std::string>{"idx0", "t"}), deleted);

    // old entries must be gone
    EXPECT_TRUE(smgr.find_entry(old_t) == nullptr);
    EXPECT_TRUE(smgr.find_entry(old_idx) == nullptr);

    // new entries must still be present
    EXPECT_TRUE(smgr.find_entry(new_t) != nullptr);
    EXPECT_TRUE(smgr.find_entry(new_idx) != nullptr);

    // table still usable after maintenance
    execute_statement("INSERT INTO t VALUES (1, 10)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", result);
    ASSERT_EQ(1, result.size());
}

// ─── Tests: TRUNCATE TABLE + sequences ───────────────────────────────────────

// After TRUNCATE TABLE (CONTINUE IDENTITY, the default) the sequence metadata
// entry count must be unchanged: the sequence is not touched at all.
TEST_F(sql_lazy_delete_new_state_test, truncate_table_continue_identity_sequence_count_unchanged) {
    auto seq_before = count_sequences(*db_);
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    EXPECT_EQ(seq_before + 1, count_sequences(*db_));

    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");

    // default is CONTINUE IDENTITY: sequence entry must not be changed
    execute_statement("TRUNCATE TABLE t");
    EXPECT_EQ(seq_before + 1, count_sequences(*db_));

    // sequence continues from 3
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c1 FROM t WHERE c0 = 10", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(3)), result[0]);
}

// After TRUNCATE TABLE RESTART IDENTITY the sequence metadata entry count must
// be unchanged (old definition_id removed, new one assigned), but the sequence
// itself restarts from its initial value.
TEST_F(sql_lazy_delete_new_state_test, truncate_table_restart_identity_sequence_count_unchanged) {
    auto seq_before = count_sequences(*db_);
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    EXPECT_EQ(seq_before + 1, count_sequences(*db_));

    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");

    // RESTART IDENTITY: old sequence entry removed, new one created
    execute_statement("TRUNCATE TABLE t RESTART IDENTITY");
    EXPECT_EQ(seq_before + 1, count_sequences(*db_));

    // sequence restarts from 1
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c1 FROM t WHERE c0 = 10", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(1)), result[0]);
}

} // namespace jogasaki::testing
