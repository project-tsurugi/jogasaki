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
#include <vector>
#include <gtest/gtest.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/id.h>
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
    if (jogasaki::kvs::implementation_id() != "memory") {
        ASSERT_EQ((std::vector<std::string>{"t"}), storage::maintenance_storage());
        EXPECT_TRUE(smgr.find_entry(e) == nullptr);
    }
}

} // namespace jogasaki::testing
