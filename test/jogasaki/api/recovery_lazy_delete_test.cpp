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
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/maintenance_storage.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/get_storage_by_index_name.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::executor;
using namespace jogasaki::mock;

using kind = meta::field_type_kind;

/**
 * @brief Tests for recovery of storages that are in a lazy-delete state
 * (i.e. DROP TABLE / DROP INDEX was performed and reserve_delete_entry() was
 * called, but delete_storage() has not yet been invoked before the server
 * was stopped).
 *
 * All tests set enable_maintenance_thread = false so that maintenance_storage()
 * must be called explicitly.  This makes the "lazy delete pending" state
 * observable between recovery and cleanup.
 */
class recovery_lazy_delete_test :
    public ::testing::Test,
    public api_test_base {

public:
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_maintenance_thread(false);  // manual maintenance in tests
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

/**
 * @brief Verify that `name` does not appear in `storage_manager`'s name map
 *        and that there is exactly one delete-reserved entry whose original_name
 *        matches `name`.  Returns that entry.
 */
static storage::storage_entry find_reserved_entry_by_original_name(
    storage::storage_manager& smgr,
    std::string_view name
) {
    EXPECT_TRUE(! smgr.find_by_name(name).has_value())
        << "name '" << name << "' should not be visible in storage_manager after DROP";

    auto candidates = smgr.get_delete_reserved_entries();
    for (auto const& entry : candidates) {
        auto ctrl = smgr.find_entry(entry);
        if (ctrl && ctrl->original_name() == name) {
            return entry;
        }
    }
    ADD_FAILURE() << "no delete-reserved entry with original_name '" << name << "' found";
    return {};
}

// ─── Tests: post-1.8 secondary index ─────────────────────────────────────────

TEST_F(recovery_lazy_delete_test, drop_index_lazy_delete_recovery) {
    // Verify that a DROP INDEX that was not yet cleaned up by delete_storage()
    // is correctly recovered: the entry remains in storage_manager with
    // delete_reserved = true, is absent from the configuration provider, and is
    // fully removed by maintenance_storage().
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");

    execute_statement("DROP INDEX idx0");

    // Verify state immediately after DROP, before stop/start.
    {
        SCOPED_TRACE("before recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));
        auto entry = find_reserved_entry_by_original_name(smgr, "idx0");
        auto ctrl = smgr.find_entry(entry);
        ASSERT_TRUE(ctrl);
        EXPECT_TRUE(ctrl->delete_reserved());
        EXPECT_TRUE(! ctrl->name().has_value());
        EXPECT_EQ("idx0", ctrl->original_name());
        EXPECT_TRUE(ctrl->storage_key().has_value());
        EXPECT_TRUE(global::db()->get_storage(ctrl->derived_storage_key()));
    }

    // Stop and restart — recovery must re-populate the delete-reserved entry.
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    // Verify state after recovery.
    {
        SCOPED_TRACE("after recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));
        auto entry = find_reserved_entry_by_original_name(smgr, "idx0");
        auto ctrl = smgr.find_entry(entry);
        ASSERT_TRUE(ctrl);
        // The entry must be marked for deletion.
        EXPECT_TRUE(ctrl->delete_reserved());
        // name_ must be cleared (not visible by name).
        EXPECT_TRUE(! ctrl->name().has_value());
        // original_name must be preserved so maintenance can log it.
        EXPECT_EQ("idx0", ctrl->original_name());
        // A post-1.8 index has an explicit storage_key (surrogate binary id).
        EXPECT_TRUE(ctrl->storage_key().has_value());
        // derived_storage_key() must return a valid key that KVS still recognises.
        EXPECT_TRUE(global::db()->get_storage(ctrl->derived_storage_key()));
        // Trigger cleanup: maintenance_storage() must delete the KVS storage and
        // remove the entry from storage_manager.
        ASSERT_EQ((std::vector<std::string>{"idx0"}), storage::maintenance_storage());
        // Entry must be gone after maintenance.
        EXPECT_TRUE(! smgr.find_entry(entry));
        // KVS storage must be gone as well.
        EXPECT_TRUE(! global::db()->get_storage(ctrl->derived_storage_key()));
    }
}

// ─── Tests: post-1.8 primary table ───────────────────────────────────────────

TEST_F(recovery_lazy_delete_test, drop_table_lazy_delete_recovery) {
    // Verify that a DROP TABLE whose delete_storage() was not yet called is
    // correctly recovered: the primary entry remains in storage_manager with
    // delete_reserved = true and is fully removed by maintenance_storage().
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT)");

    execute_statement("DROP TABLE t");

    // Verify state immediately after DROP, before stop/start.
    {
        SCOPED_TRACE("before recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        auto entry = find_reserved_entry_by_original_name(smgr, "t");
        auto ctrl = smgr.find_entry(entry);
        ASSERT_TRUE(ctrl);
        EXPECT_TRUE(ctrl->delete_reserved());
        EXPECT_TRUE(! ctrl->name().has_value());
        EXPECT_EQ("t", ctrl->original_name());
        EXPECT_TRUE(ctrl->storage_key().has_value());
        EXPECT_TRUE(global::db()->get_storage(ctrl->derived_storage_key()));
    }

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    // Verify state after recovery.
    {
        SCOPED_TRACE("after recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        auto entry = find_reserved_entry_by_original_name(smgr, "t");
        auto ctrl = smgr.find_entry(entry);
        ASSERT_TRUE(ctrl);
        EXPECT_TRUE(ctrl->delete_reserved());
        EXPECT_TRUE(! ctrl->name().has_value());
        EXPECT_EQ("t", ctrl->original_name());
        // post-1.8 primary index has an explicit storage_key.
        EXPECT_TRUE(ctrl->storage_key().has_value());
        // KVS storage must still exist (not yet deleted).
        EXPECT_TRUE(global::db()->get_storage(ctrl->derived_storage_key()));
        // Trigger cleanup.
        ASSERT_EQ((std::vector<std::string>{"t"}), storage::maintenance_storage());
        EXPECT_TRUE(! smgr.find_entry(entry));
        EXPECT_TRUE(! global::db()->get_storage(ctrl->derived_storage_key()));
    }
}

// ─── Tests: post-1.8 table with secondary index, DROP TABLE ──────────────────

TEST_F(recovery_lazy_delete_test, drop_table_with_secondary_index_lazy_delete_recovery) {
    // Verify that DROP TABLE marks both the primary and all secondary indices
    // as delete-reserved, and that after recovery and maintenance both are gone.
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");

    execute_statement("DROP TABLE t");

    // Verify state immediately after DROP, before stop/start.
    {
        SCOPED_TRACE("before recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));
        auto primary_entry = find_reserved_entry_by_original_name(smgr, "t");
        auto secondary_entry = find_reserved_entry_by_original_name(smgr, "idx0");
        auto primary_ctrl = smgr.find_entry(primary_entry);
        ASSERT_TRUE(primary_ctrl);
        EXPECT_TRUE(primary_ctrl->delete_reserved());
        EXPECT_TRUE(primary_ctrl->is_primary());
        EXPECT_TRUE(primary_ctrl->storage_key().has_value());
        auto secondary_ctrl = smgr.find_entry(secondary_entry);
        ASSERT_TRUE(secondary_ctrl);
        EXPECT_TRUE(secondary_ctrl->delete_reserved());
        EXPECT_TRUE(! secondary_ctrl->is_primary());
        EXPECT_TRUE(secondary_ctrl->storage_key().has_value());
    }

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    // Verify state after recovery.
    {
        SCOPED_TRACE("after recovery");
        auto& smgr = *global::storage_manager();
        // Neither primary table nor its secondary index must be visible in provider.
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));

        // Both entries must be delete-reserved.
        auto primary_entry = find_reserved_entry_by_original_name(smgr, "t");
        auto secondary_entry = find_reserved_entry_by_original_name(smgr, "idx0");

        auto primary_ctrl = smgr.find_entry(primary_entry);
        ASSERT_TRUE(primary_ctrl);
        EXPECT_TRUE(primary_ctrl->delete_reserved());
        EXPECT_TRUE(primary_ctrl->is_primary());
        EXPECT_TRUE(primary_ctrl->storage_key().has_value());

        auto secondary_ctrl = smgr.find_entry(secondary_entry);
        ASSERT_TRUE(secondary_ctrl);
        EXPECT_TRUE(secondary_ctrl->delete_reserved());
        EXPECT_TRUE(! secondary_ctrl->is_primary());
        EXPECT_TRUE(secondary_ctrl->storage_key().has_value());

        // Trigger cleanup — both storages must be removed.
        auto deleted = storage::maintenance_storage();
        std::sort(deleted.begin(), deleted.end());
        ASSERT_EQ((std::vector<std::string>{"idx0", "t"}), deleted);

        EXPECT_TRUE(! smgr.find_entry(primary_entry));
        EXPECT_TRUE(! smgr.find_entry(secondary_entry));
    }
}

// ─── Tests: pre-1.8 secondary index (no storage_key in metadata) ─────────────

TEST_F(recovery_lazy_delete_test, pre18_drop_index_lazy_delete_recovery) {
    // Verify that a pre-1.8 secondary index (created without a storage_key) that
    // has been dropped but not yet deleted is correctly recovered:
    //  * The entry has no explicit storage_key (storage_key() == nullopt).
    //  * derived_storage_key() falls back to original_name, which is the plain
    //    index name used as the KVS key in pre-1.8.
    //  * maintenance_storage() can still find and delete the KVS storage.
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    // Create index using pre-1.8 behaviour (no storage_key stored in metadata).
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");
    global::config_pool()->enable_storage_key(true);

    execute_statement("DROP INDEX idx0");

    // Verify state immediately after DROP, before stop/start.
    // reserve_delete_entry() promotes the plain name to storage_key for pre-1.8 entries.
    {
        SCOPED_TRACE("before recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));
        auto entry = find_reserved_entry_by_original_name(smgr, "idx0");
        auto ctrl = smgr.find_entry(entry);
        ASSERT_TRUE(ctrl);
        EXPECT_TRUE(ctrl->delete_reserved());
        EXPECT_TRUE(! ctrl->name().has_value());
        EXPECT_EQ("idx0", ctrl->original_name());
        ASSERT_TRUE(ctrl->storage_key().has_value());
        EXPECT_EQ("idx0", *ctrl->storage_key());
        EXPECT_EQ("idx0", ctrl->derived_storage_key());
        EXPECT_TRUE(global::db()->get_storage("idx0"));
    }

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    // Verify state after recovery.
    {
        SCOPED_TRACE("after recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));
        auto entry = find_reserved_entry_by_original_name(smgr, "idx0");
        auto ctrl = smgr.find_entry(entry);
        ASSERT_TRUE(ctrl);
        EXPECT_TRUE(ctrl->delete_reserved());
        EXPECT_TRUE(! ctrl->name().has_value());
        EXPECT_EQ("idx0", ctrl->original_name());
        // Pre-1.8 entries have no storage_key before DROP, but set_storage_option_delete_reserved
        // writes the plain name as storage_key into the KVS proto during DROP.  After recovery
        // the entry therefore has storage_key set to the plain name.
        ASSERT_TRUE(ctrl->storage_key().has_value());
        EXPECT_EQ("idx0", *ctrl->storage_key());
        EXPECT_EQ("idx0", ctrl->derived_storage_key());
        // KVS storage must still exist under the plain name.
        EXPECT_TRUE(global::db()->get_storage("idx0"));
        // Trigger cleanup.
        ASSERT_EQ((std::vector<std::string>{"idx0"}), storage::maintenance_storage());
        EXPECT_TRUE(! smgr.find_entry(entry));
        EXPECT_TRUE(! global::db()->get_storage("idx0"));
    }
}

// ─── Tests: pre-1.8 primary table (no storage_key in metadata) ───────────────

TEST_F(recovery_lazy_delete_test, pre18_drop_table_lazy_delete_recovery) {
    // Verify that a pre-1.8 primary index (table) that has been dropped but not
    // yet deleted is correctly recovered and cleaned up by maintenance_storage().
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT)");
    global::config_pool()->enable_storage_key(true);

    execute_statement("DROP TABLE t");

    // Verify state immediately after DROP, before stop/start.
    // reserve_delete_entry() promotes the plain name to storage_key for pre-1.8 entries.
    {
        SCOPED_TRACE("before recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        auto entry = find_reserved_entry_by_original_name(smgr, "t");
        auto ctrl = smgr.find_entry(entry);
        ASSERT_TRUE(ctrl);
        EXPECT_TRUE(ctrl->delete_reserved());
        EXPECT_TRUE(! ctrl->name().has_value());
        EXPECT_EQ("t", ctrl->original_name());
        ASSERT_TRUE(ctrl->storage_key().has_value());
        EXPECT_EQ("t", *ctrl->storage_key());
        EXPECT_EQ("t", ctrl->derived_storage_key());
        EXPECT_TRUE(global::db()->get_storage("t"));
    }

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    // Verify state after recovery.
    {
        SCOPED_TRACE("after recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        auto entry = find_reserved_entry_by_original_name(smgr, "t");
        auto ctrl = smgr.find_entry(entry);
        ASSERT_TRUE(ctrl);
        EXPECT_TRUE(ctrl->delete_reserved());
        EXPECT_TRUE(! ctrl->name().has_value());
        EXPECT_EQ("t", ctrl->original_name());
        // Pre-1.8 entries have no storage_key before DROP, but set_storage_option_delete_reserved
        // writes the plain name as storage_key into the KVS proto during DROP.  After recovery
        // the entry therefore has storage_key set to the plain name.
        ASSERT_TRUE(ctrl->storage_key().has_value());
        EXPECT_EQ("t", *ctrl->storage_key());
        EXPECT_EQ("t", ctrl->derived_storage_key());
        // KVS storage must still exist under the plain name.
        EXPECT_TRUE(global::db()->get_storage("t"));
        // Trigger cleanup.
        ASSERT_EQ((std::vector<std::string>{"t"}), storage::maintenance_storage());
        EXPECT_TRUE(! smgr.find_entry(entry));
        EXPECT_TRUE(! global::db()->get_storage("t"));
    }
}

// ─── Tests: pre-1.8 table with secondary index, DROP TABLE ───────────────────

TEST_F(recovery_lazy_delete_test, pre18_drop_table_with_secondary_index_lazy_delete_recovery) {
    // Verify that pre-1.8 DROP TABLE (primary + secondary, both without
    // storage_key) is handled correctly across recovery and maintenance.
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT)");
    execute_statement("CREATE INDEX idx0 ON t (c1)");
    global::config_pool()->enable_storage_key(true);

    execute_statement("DROP TABLE t");

    // Verify state immediately after DROP, before stop/start.
    // reserve_delete_entry() promotes plain names to storage_key for pre-1.8 entries.
    {
        SCOPED_TRACE("before recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));
        auto primary_entry = find_reserved_entry_by_original_name(smgr, "t");
        auto secondary_entry = find_reserved_entry_by_original_name(smgr, "idx0");
        auto primary_ctrl = smgr.find_entry(primary_entry);
        ASSERT_TRUE(primary_ctrl);
        EXPECT_TRUE(primary_ctrl->delete_reserved());
        ASSERT_TRUE(primary_ctrl->storage_key().has_value());
        EXPECT_EQ("t", *primary_ctrl->storage_key());
        EXPECT_EQ("t", primary_ctrl->derived_storage_key());
        auto secondary_ctrl = smgr.find_entry(secondary_entry);
        ASSERT_TRUE(secondary_ctrl);
        EXPECT_TRUE(secondary_ctrl->delete_reserved());
        ASSERT_TRUE(secondary_ctrl->storage_key().has_value());
        EXPECT_EQ("idx0", *secondary_ctrl->storage_key());
        EXPECT_EQ("idx0", secondary_ctrl->derived_storage_key());
        EXPECT_TRUE(global::db()->get_storage("t"));
        EXPECT_TRUE(global::db()->get_storage("idx0"));
    }

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    // Verify state after recovery.
    {
        SCOPED_TRACE("after recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));

        auto primary_entry = find_reserved_entry_by_original_name(smgr, "t");
        auto secondary_entry = find_reserved_entry_by_original_name(smgr, "idx0");

        auto primary_ctrl = smgr.find_entry(primary_entry);
        ASSERT_TRUE(primary_ctrl);
        EXPECT_TRUE(primary_ctrl->delete_reserved());
        // Pre-1.8: storage_key is written as plain name by set_storage_option_delete_reserved during DROP.
        ASSERT_TRUE(primary_ctrl->storage_key().has_value());
        EXPECT_EQ("t", *primary_ctrl->storage_key());
        EXPECT_EQ("t", primary_ctrl->derived_storage_key());

        auto secondary_ctrl = smgr.find_entry(secondary_entry);
        ASSERT_TRUE(secondary_ctrl);
        EXPECT_TRUE(secondary_ctrl->delete_reserved());
        ASSERT_TRUE(secondary_ctrl->storage_key().has_value());
        EXPECT_EQ("idx0", *secondary_ctrl->storage_key());
        EXPECT_EQ("idx0", secondary_ctrl->derived_storage_key());

        // Both KVS storages must still exist before maintenance.
        EXPECT_TRUE(global::db()->get_storage("t"));
        EXPECT_TRUE(global::db()->get_storage("idx0"));

        // Trigger cleanup.
        auto deleted = storage::maintenance_storage();
        std::sort(deleted.begin(), deleted.end());
        ASSERT_EQ((std::vector<std::string>{"idx0", "t"}), deleted);

        EXPECT_TRUE(! smgr.find_entry(primary_entry));
        EXPECT_TRUE(! smgr.find_entry(secondary_entry));
        EXPECT_TRUE(! global::db()->get_storage("t"));
        EXPECT_TRUE(! global::db()->get_storage("idx0"));
    }
}

// ─── Tests: mixed pre-1.8 primary table + post-1.8 secondary index ───────────

TEST_F(recovery_lazy_delete_test, mixed_pre18_table_post18_index_drop_table_lazy_delete_recovery) {
    // Verify that DROP TABLE works correctly when the primary index was created
    // in pre-1.8 style (no storage_key) and the secondary index was created in
    // post-1.8 style (with a surrogate storage_key).
    //
    // Expected behaviour:
    //  * Primary entry: storage_key promoted from plain name "t" by
    //    reserve_delete_entry().  After recovery storage_key == "t".
    //  * Secondary entry: surrogate binary storage_key preserved across recovery.
    //  * maintenance_storage() removes both.
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    global::config_pool()->enable_storage_key(false);
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT)");
    global::config_pool()->enable_storage_key(true);
    execute_statement("CREATE INDEX idx0 ON t (c1)");

    execute_statement("DROP TABLE t");

    // Verify state immediately after DROP, before stop/start.
    {
        SCOPED_TRACE("before recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));
        auto primary_entry = find_reserved_entry_by_original_name(smgr, "t");
        auto secondary_entry = find_reserved_entry_by_original_name(smgr, "idx0");
        auto primary_ctrl = smgr.find_entry(primary_entry);
        ASSERT_TRUE(primary_ctrl);
        EXPECT_TRUE(primary_ctrl->delete_reserved());
        // Primary was pre-1.8: reserve_delete_entry() promotes plain name to storage_key.
        ASSERT_TRUE(primary_ctrl->storage_key().has_value());
        EXPECT_EQ("t", *primary_ctrl->storage_key());
        EXPECT_EQ("t", primary_ctrl->derived_storage_key());
        EXPECT_TRUE(global::db()->get_storage("t"));
        auto secondary_ctrl = smgr.find_entry(secondary_entry);
        ASSERT_TRUE(secondary_ctrl);
        EXPECT_TRUE(secondary_ctrl->delete_reserved());
        // Secondary was post-1.8: already has a surrogate binary storage_key.
        EXPECT_TRUE(secondary_ctrl->storage_key().has_value());
        EXPECT_TRUE(global::db()->get_storage(secondary_ctrl->derived_storage_key()));
    }

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    // Verify state after recovery.
    {
        SCOPED_TRACE("after recovery");
        auto& smgr = *global::storage_manager();
        EXPECT_TRUE(! db_impl()->tables()->find_table("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("t"));
        EXPECT_TRUE(! db_impl()->tables()->find_index("idx0"));
        auto primary_entry = find_reserved_entry_by_original_name(smgr, "t");
        auto secondary_entry = find_reserved_entry_by_original_name(smgr, "idx0");

        auto primary_ctrl = smgr.find_entry(primary_entry);
        ASSERT_TRUE(primary_ctrl);
        EXPECT_TRUE(primary_ctrl->delete_reserved());
        // Pre-1.8 primary: set_storage_option_delete_reserved writes "t" as storage_key into proto.
        ASSERT_TRUE(primary_ctrl->storage_key().has_value());
        EXPECT_EQ("t", *primary_ctrl->storage_key());
        EXPECT_EQ("t", primary_ctrl->derived_storage_key());
        EXPECT_TRUE(global::db()->get_storage("t"));

        auto secondary_ctrl = smgr.find_entry(secondary_entry);
        ASSERT_TRUE(secondary_ctrl);
        EXPECT_TRUE(secondary_ctrl->delete_reserved());
        // Post-1.8 secondary: surrogate storage_key preserved across recovery.
        EXPECT_TRUE(secondary_ctrl->storage_key().has_value());
        EXPECT_TRUE(global::db()->get_storage(secondary_ctrl->derived_storage_key()));

        // Trigger cleanup.
        auto deleted = storage::maintenance_storage();
        std::sort(deleted.begin(), deleted.end());
        ASSERT_EQ((std::vector<std::string>{"idx0", "t"}), deleted);

        EXPECT_TRUE(! smgr.find_entry(primary_entry));
        EXPECT_TRUE(! smgr.find_entry(secondary_entry));
        EXPECT_TRUE(! global::db()->get_storage("t"));
        EXPECT_TRUE(! global::db()->get_storage(secondary_ctrl->derived_storage_key()));
    }
}


// ─── Tests: rowid table (no explicit PK) re-created before recovery ───────────

// Return the number of entries in the sequence system table.
static std::size_t count_sequences(api::database& db) {
    auto tx = utils::create_transaction(db);
    auto tctx = get_transaction_context(*tx);
    executor::sequence::metadata_store ms{*tctx->object()};
    std::size_t count = 0;
    ms.scan([&count](std::size_t, std::size_t) { ++count; });
    return count;
}

TEST_F(recovery_lazy_delete_test, drop_rowid_table_recreate_and_recovery) {
    // Verify that a dropped rowid table (lazy-delete state) is correctly recovered
    // on restart.  After recovery the table can be re-created with the same name
    // without sequence conflicts, and maintenance_storage() cleans up the old entry.
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    auto seq_count_before = count_sequences(*db_);

    // no explicit PK → one rowid sequence generated
    execute_statement("CREATE TABLE t (c0 INT)");
    EXPECT_EQ(seq_count_before + 1, count_sequences(*db_));

    // Insert multiple rows before DROP.
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("INSERT INTO t (c0) VALUES (3)");

    execute_statement("DROP TABLE t");
    EXPECT_EQ(seq_count_before, count_sequences(*db_));

    // stop/start in the dropped state — test recovery of the lazy-delete entry.
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    // After recovery: delete-reserved entry must be present, sequence must be gone.
    EXPECT_EQ(seq_count_before, count_sequences(*db_));

    auto& smgr = *global::storage_manager();
    EXPECT_TRUE(! smgr.find_by_name("t").has_value());

    auto reserved_e = find_reserved_entry_by_original_name(smgr, "t");
    auto reserved_ctrl = smgr.find_entry(reserved_e);
    ASSERT_TRUE(reserved_ctrl != nullptr);
    EXPECT_TRUE(reserved_ctrl->delete_reserved());
    EXPECT_TRUE(global::db()->get_storage(reserved_ctrl->derived_storage_key()));

    // Re-create the table after recovery and insert rows.
    execute_statement("CREATE TABLE t (c0 INT)");
    EXPECT_EQ(seq_count_before + 1, count_sequences(*db_));
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    execute_statement("INSERT INTO t (c0) VALUES (20)");
    execute_statement("INSERT INTO t (c0) VALUES (30)");

    // maintenance_storage() must delete only the old reserved storage.
    ASSERT_EQ((std::vector<std::string>{"t"}), storage::maintenance_storage());
    EXPECT_TRUE(smgr.find_entry(reserved_e) == nullptr);
}

// ─── Tests: GENERATED ALWAYS AS IDENTITY table re-created before recovery ────

TEST_F(recovery_lazy_delete_test, drop_identity_table_recreate_and_recovery) {
    // Verify that a dropped identity-column table (lazy-delete state) is correctly
    // recovered on restart.  After recovery the table can be re-created with the
    // same name without sequence conflicts, and maintenance_storage() cleans up
    // the old entry.
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    auto seq_count_before = count_sequences(*db_);

    // c1 is GENERATED ALWAYS AS IDENTITY → one identity sequence generated
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    EXPECT_EQ(seq_count_before + 1, count_sequences(*db_));

    // Insert multiple rows before DROP.
    execute_statement("INSERT INTO t (c0) VALUES (1)");
    execute_statement("INSERT INTO t (c0) VALUES (2)");
    execute_statement("INSERT INTO t (c0) VALUES (3)");

    execute_statement("DROP TABLE t");
    EXPECT_EQ(seq_count_before, count_sequences(*db_));

    // stop/start in the dropped state — test recovery of the lazy-delete entry.
    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    // After recovery: delete-reserved entry must be present, sequence must be gone.
    EXPECT_EQ(seq_count_before, count_sequences(*db_));

    auto& smgr = *global::storage_manager();
    EXPECT_TRUE(! smgr.find_by_name("t").has_value());

    auto reserved_e = find_reserved_entry_by_original_name(smgr, "t");
    auto reserved_ctrl = smgr.find_entry(reserved_e);
    ASSERT_TRUE(reserved_ctrl != nullptr);
    EXPECT_TRUE(reserved_ctrl->delete_reserved());
    EXPECT_TRUE(global::db()->get_storage(reserved_ctrl->derived_storage_key()));

    // Re-create the table after recovery and insert rows.
    execute_statement("CREATE TABLE t (c0 INT NOT NULL PRIMARY KEY, c1 INT GENERATED ALWAYS AS IDENTITY)");
    EXPECT_EQ(seq_count_before + 1, count_sequences(*db_));
    execute_statement("INSERT INTO t (c0) VALUES (10)");
    execute_statement("INSERT INTO t (c0) VALUES (20)");
    execute_statement("INSERT INTO t (c0) VALUES (30)");

    // maintenance_storage() must delete only the old reserved storage.
    ASSERT_EQ((std::vector<std::string>{"t"}), storage::maintenance_storage());
    EXPECT_TRUE(smgr.find_entry(reserved_e) == nullptr);
}

} // namespace jogasaki::testing
