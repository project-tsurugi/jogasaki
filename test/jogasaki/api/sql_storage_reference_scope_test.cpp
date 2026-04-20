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

#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
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
 * @brief Integration tests verifying that DML statements correctly increment the
 * ref_transaction_count on the accessed storage entries (Step 3 of tsurugi-issues #177).
 *
 * These tests require shirakami implementation. They are skipped on memory backend.
 */
class sql_storage_reference_scope_test :
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

// helper: return ref_transaction_count for a table by name
static std::size_t ref_count(std::string_view table_name) {
    auto& smgr = *global::storage_manager();
    auto entry = smgr.find_by_name(table_name);
    if (! entry) {
        return 0;
    }
    auto ctrl = smgr.find_entry(*entry);
    if (! ctrl) {
        return 0;
    }
    return ctrl->ref_transaction_count();
}

TEST_F(sql_storage_reference_scope_test, select_increments_ref_count_during_tx) {
    if (kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "ref_transaction_count lifecycle not testable with memory implementation";
    }
    // After a SELECT inside a tx, ref_transaction_count for the table must be 1.
    // After the tx commits, ref_transaction_count must return to 0.
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (1)");

    utils::set_global_tx_option(utils::create_tx_option{false, true});  // OCC
    auto tx = utils::create_transaction(*db_);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    ASSERT_EQ(1, result.size());

    EXPECT_EQ(1, ref_count("t"));

    ASSERT_EQ(status::ok, tx->commit());
    EXPECT_EQ(0, ref_count("t"));
}

TEST_F(sql_storage_reference_scope_test, insert_increments_ref_count_during_tx) {
    if (kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "ref_transaction_count lifecycle not testable with memory implementation";
    }
    // An INSERT also increments ref_transaction_count.
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");

    utils::set_global_tx_option(utils::create_tx_option{false, true});  // OCC
    auto tx = utils::create_transaction(*db_);
    execute_statement("INSERT INTO t VALUES (42)", *tx);

    EXPECT_EQ(1, ref_count("t"));

    ASSERT_EQ(status::ok, tx->commit());
    EXPECT_EQ(0, ref_count("t"));
}

TEST_F(sql_storage_reference_scope_test, same_table_accessed_multiple_times_counts_once) {
    if (kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "ref_transaction_count lifecycle not testable with memory implementation";
    }
    // Executing multiple DML statements against the same table in one tx
    // must keep ref_transaction_count at 1 (idempotent).
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (2)");

    utils::set_global_tx_option(utils::create_tx_option{false, true});  // OCC
    auto tx = utils::create_transaction(*db_);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    execute_query("SELECT * FROM t", *tx, result);
    execute_statement("INSERT INTO t VALUES (3)", *tx);

    EXPECT_EQ(1, ref_count("t"));

    ASSERT_EQ(status::ok, tx->commit());
    EXPECT_EQ(0, ref_count("t"));
}

TEST_F(sql_storage_reference_scope_test, abort_also_decrements_ref_count) {
    if (kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "ref_transaction_count lifecycle not testable with memory implementation";
    }
    // Aborting the tx must also decrement ref_transaction_count to 0.
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");

    utils::set_global_tx_option(utils::create_tx_option{false, true});  // OCC
    auto tx = utils::create_transaction(*db_);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);

    EXPECT_EQ(1, ref_count("t"));

    ASSERT_EQ(status::ok, tx->abort());
    EXPECT_EQ(0, ref_count("t"));
}

TEST_F(sql_storage_reference_scope_test, two_concurrent_txs_accumulate_ref_count) {
    if (kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "ref_transaction_count lifecycle not testable with memory implementation";
    }
    // Two open transactions accessing the same table must each contribute 1 to
    // ref_transaction_count, for a total of 2.
    execute_statement("CREATE TABLE t (c0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (1)");

    utils::set_global_tx_option(utils::create_tx_option{false, true});  // OCC
    auto tx1 = utils::create_transaction(*db_);
    auto tx2 = utils::create_transaction(*db_);

    std::vector<mock::basic_record> r1{};
    execute_query("SELECT * FROM t", *tx1, r1);
    std::vector<mock::basic_record> r2{};
    execute_query("SELECT * FROM t", *tx2, r2);

    EXPECT_EQ(2, ref_count("t"));

    ASSERT_EQ(status::ok, tx1->commit());
    EXPECT_EQ(1, ref_count("t"));

    ASSERT_EQ(status::ok, tx2->commit());
    EXPECT_EQ(0, ref_count("t"));
}

} // namespace jogasaki::testing
