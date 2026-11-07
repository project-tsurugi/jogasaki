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
 * the abort message must resolve the logical index name before decoding the key.
 * These tests verify both the decoded key and the index label.
 */

#include <atomic>
#include <memory>
#include <sstream>
#include <string>

#include <gtest/gtest.h>
#include <sharksfin/CallResult.h>
#include <sharksfin/ErrorLocator.h>

#include <jogasaki/api/error_info.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/abort_error.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/get_storage_by_index_name.h>

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
TEST_F(abort_message_storage_name_test, ltx_write_conflict_shows_key_and_index_name) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2)");

    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    auto failed_tx = db_impl()->find_transaction(*tx2);

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

    // Both the decoded key and the human-readable index name must be shown.
    EXPECT_NE(std::string::npos, msg.find("location={key:(0:int4*)[1] index:T0}")) << msg;

    // An unavailable definition must retain the binary key instead of guessing a schema.
    request_context context{
        {}, std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()), {}, failed_tx};
    context.storage_provider(std::make_shared<yugawara::storage::configurable_provider>());
    auto result = failed_tx->recent_call_result();
    ASSERT_TRUE(result);
    ASSERT_TRUE(result->location());
    ASSERT_EQ(sharksfin::ErrorLocatorKind::storage_key, result->location()->kind());
    auto const& locator = static_cast<sharksfin::StorageKeyErrorLocator const&>(*result->location());
    ASSERT_TRUE(locator.key());
    std::ostringstream expected{};
    expected << "location={key:" << utils::binary_printer{locator.key()->data(), locator.key()->size()}
             << " index:T0}";
    auto fallback_message = utils::create_abort_message(context);
    EXPECT_NE(std::string::npos, fallback_message.find(expected.str())) << fallback_message;
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
TEST_F(abort_message_storage_name_test, occ_read_conflict_on_secondary_shows_key_and_index_name) {
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

    EXPECT_NE(std::string::npos, msg.find("location={key:(0:int4*)[100] index:I0}")) << msg;
}


TEST_F(abort_message_storage_name_test, ltx_conflict_decodes_composite_character_key) {
    execute_statement("CREATE TABLE T1 (C0 VARCHAR(100) NOT NULL, C1 INT NOT NULL, V INT, PRIMARY KEY(C0, C1))");
    execute_statement("INSERT INTO T1 VALUES ('key-with-more-than-sixteen-characters', 205, 1)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T1"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T1"});
    execute_statement("UPDATE T1 SET V=10 WHERE C0='key-with-more-than-sixteen-characters' AND C1=205", *tx1);
    execute_statement("UPDATE T1 SET V=20 WHERE C0='key-with-more-than-sixteen-characters' AND C1=205", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    std::shared_ptr<api::error_info> info{};
    ASSERT_EQ(status::err_serialization_failure, commit_and_get_error(tx2, info));
    ASSERT_TRUE(info);
    std::string message{info->message()};
    EXPECT_NE(std::string::npos, message.find("[key-with-more-than-sixteen-characters] (1:int4*)[205] index:T1}")) << message;
}

TEST_F(abort_message_storage_name_test, occ_conflict_decodes_null_secondary_key) {
    execute_statement("CREATE INDEX I0 ON T0 (C1)");
    execute_statement("INSERT INTO T0 VALUES (205, NULL)");
    auto tx = utils::create_transaction(*db_, false, false);
    // Read I0 directly so the NULL secondary key enters the OCC read set.
    // SQL IS NULL selects a primary-index scan in this case.
    auto index = utils::get_storage_by_index_name("I0");
    ASSERT_TRUE(index);
    auto context = db_impl()->find_transaction(*tx);
    std::unique_ptr<kvs::iterator> it{};
    ASSERT_EQ(status::ok, index->content_scan(*context->object(), {}, kvs::end_point_kind::unbound,
        {}, kvs::end_point_kind::unbound, it));
    ASSERT_EQ(status::ok, it->next());
    std::string_view key{};
    std::string_view value{};
    ASSERT_EQ(status::ok, it->read_key(key));
    // Reading the value registers the existing entry for OCC verification.
    ASSERT_EQ(status::ok, it->read_value(value));
    it.reset();
    execute_statement("UPDATE T0 SET C1=100 WHERE C0=205");
    std::shared_ptr<api::error_info> info{};
    ASSERT_EQ(status::err_serialization_failure, commit_and_get_error(tx, info));
    ASSERT_TRUE(info);
    std::string message{info->message()};
    EXPECT_NE(std::string::npos, message.find("location={key:(0:int4*)[-] index:I0}")) << message;
}

}  // namespace jogasaki::testing
