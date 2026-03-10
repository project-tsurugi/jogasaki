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

/**
 * @file abort_message_storage_name_test.cpp
 * @brief Tests for issue #1388 https://github.com/project-tsurugi/tsurugi-issues/issues/1388
 *
 * Since issue #1380, the KVS storage key is a binary surrogate ID (not the table name).
 * When a serialization failure occurs and sharksfin provides a StorageKeyErrorLocator,
 * the abort message currently includes the raw binary key as "storage:<binary>".
 * After the fix the message should instead contain "index:<index_name>".
 */

#include <atomic>
#include <memory>
#include <string>

#include <gtest/gtest.h>

#include <jogasaki/api/error_info.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/create_tx.h>

#include "../api/api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;

/**
 * @brief Test fixture for abort message storage name tests
 */
class abort_message_storage_name_test :
    public ::testing::Test,
    public api_test_base {
public:
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
        execute_statement("CREATE TABLE T0 (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    }

    void TearDown() override {
        db_teardown();
    }

    /**
     * @brief commit tx asynchronously and wait for the result
     * @param tx transaction handle to commit
     * @param out_info [out] error_info if commit failed
     * @return status from the commit callback
     */
    static status commit_and_get_error(
        std::shared_ptr<api::transaction_handle> const& tx,
        std::shared_ptr<api::error_info>& out_info
    ) {
        std::atomic<bool> done{false};
        status result{};
        tx->commit_async([&](status st, std::shared_ptr<api::error_info> info) {
            result = st;
            out_info = std::move(info);
            done.store(true);
        });
        while (! done.load()) {}
        return result;
    }
};

/**
 * @brief Reproduce issue #1388: LTX write conflict abort message contains binary storage key.
 *
 * Two LTX transactions both update the same row in T0.
 * The second commit fails with err_serialization_failure.
 * The error message should contain "index:T0" (human-readable index name),
 * NOT a raw binary surrogate ID.
 */
TEST_F(abort_message_storage_name_test, ltx_write_conflict_shows_index_name) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2)");

    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});

    execute_statement("UPDATE T0 SET C1=10 WHERE C0=1", *tx1);
    execute_statement("UPDATE T0 SET C1=20 WHERE C0=1", *tx2);

    // tx1 commits successfully
    ASSERT_EQ(status::ok, tx1->commit());

    // tx2 commit fails due to LTX write conflict (CC_LTX_WRITE_ERROR)
    // StorageKeyErrorLocator is set by shirakami with the binary surrogate ID of T0
    std::shared_ptr<api::error_info> err_info{};
    auto commit_status = commit_and_get_error(tx2, err_info);

    ASSERT_EQ(status::err_serialization_failure, commit_status);
    ASSERT_TRUE(err_info);

    std::string msg{err_info->message()};
    std::cerr << "abort message: " << msg << std::endl;

    // The message must mention the human-readable index name "T0", not a raw binary key.
    // Pre-fix this assertion FAILS because the message says "storage:<binary>" instead.
    EXPECT_TRUE(msg.find("index:T0") != std::string::npos)
        << "Expected message to contain 'index:T0' but got: " << msg;
}
/**
 * @brief Verify that abort messages show the secondary index name.
 *
 * Scenario (CC_OCC_READ_VERIFY on a secondary index):
 *   1. Create secondary index I0 on T0(C1), insert one row (C0=1, C1=100).
 *   2. An OCC transaction reads the row via the secondary index (WHERE C1=100).
 *      This registers I0's key (100,1) in the OCC read set (before T0's row).
 *   3. A committed UPDATE changes C1 from 100 to 200, invalidating I0's entry.
 *   4. The OCC transaction commits: shirakami reports CC_OCC_READ_VERIFY with
 *      I0's storage key in the locator (I0's key is the first read in the set).
 *
 * Expected: the abort message contains "index:I0".
 */
TEST_F(abort_message_storage_name_test, occ_read_conflict_on_secondary_shows_index_name) {
    execute_statement("CREATE INDEX I0 ON T0 (C1)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 100)");

    // OCC transaction reads via secondary index — I0's key is first in the read set.
    auto tx_occ = utils::create_transaction(*db_, false, false);
    std::vector<mock::basic_record> initial_result{};
    execute_query("SELECT * FROM T0 WHERE C1 = 100", *tx_occ, initial_result);
    ASSERT_EQ(1, initial_result.size());

    // Committed UPDATE changes C1=100 to C1=200, invalidating I0's key (100,1).
    execute_statement("UPDATE T0 SET C1 = 200 WHERE C0 = 1");

    // tx_occ commit fails: CC_OCC_READ_VERIFY is reported with I0's storage key.
    std::shared_ptr<api::error_info> err_info{};
    auto commit_status = commit_and_get_error(tx_occ, err_info);

    ASSERT_EQ(status::err_serialization_failure, commit_status);
    ASSERT_TRUE(err_info);

    std::string msg{err_info->message()};
    std::cerr << "abort message: " << msg << std::endl;

    EXPECT_TRUE(msg.find("index:I0") != std::string::npos)
        << "Expected message to contain 'index:I0' but got: " << msg;
}

}  // namespace jogasaki::testing
