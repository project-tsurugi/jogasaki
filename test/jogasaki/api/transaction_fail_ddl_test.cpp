/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>

#include "../api/api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

/**
 * @brief test the sisuation that transaction used for DDL is aborted
 */
class transaction_fail_ddl_test :
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
        utils::set_global_tx_option(utils::create_tx_option{false, true});
    }

    void TearDown() override {
        db_teardown();
    }
    std::size_t seq_count();
    std::unordered_map<std::size_t, std::size_t> seq_list();
    bool exists_seq(std::size_t seq_id);
    bool remove_seq(std::size_t seq_id);
};

using namespace std::string_view_literals;

std::size_t transaction_fail_ddl_test::seq_count() {
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);
    executor::sequence::metadata_store ms{*tctx->object()};
    return ms.size();
}

std::unordered_map<std::size_t, std::size_t> transaction_fail_ddl_test::seq_list() {
    auto tx = utils::create_transaction(*db_);
    auto tctx = get_transaction_context(*tx);
    executor::sequence::metadata_store ms{*tctx->object()};
    std::unordered_map<std::size_t, std::size_t> ret{};
    ms.scan([&ret](std::size_t def_id, std::size_t id) {
        ret[def_id] = id;
    });
    return ret;
}

bool transaction_fail_ddl_test::exists_seq(std::size_t seq_id) {
    sequence_versioned_value value{};
    return api::impl::get_impl(*db_).kvs_db()->read_sequence(seq_id, value) != status::err_not_found;
}

bool transaction_fail_ddl_test::remove_seq(std::size_t seq_id) {
    return api::impl::get_impl(*db_).kvs_db()->delete_sequence(seq_id) == status::ok;
}

TEST_F(transaction_fail_ddl_test, create_simple_table) {
    // simple table with primary key and no identity column will not use sequence or sequences system table, so
    // aborting transaction will not affect the table creation
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("CREATE TABLE t (c0 int primary key)", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    execute_statement("INSERT INTO t VALUES (1)");
    ASSERT_EQ(0, seq_count());

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());
    execute_statement("INSERT INTO t VALUES (2)");
    ASSERT_EQ(0, seq_count());
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t", result);
        ASSERT_EQ(2, result.size());
    }
    execute_statement("DROP TABLE t");
    execute_statement("CREATE TABLE t (c0 int primary key)");
    ASSERT_EQ(0, seq_count());
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("DROP TABLE t");
}

TEST_F(transaction_fail_ddl_test, create_pkless_table) {
    // verify aborting transaction causes failure in storing sequence system table
    // it appears DML works as in-memory object is used just after the DDL, but it fails to work after re-start.
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("CREATE TABLE t (c0 int)", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    execute_statement("INSERT INTO t VALUES (1)"); // using in-memory sequence, this dml won't fail
    ASSERT_EQ(0, seq_count()); // no entry due to abort

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());  // warning message should be shown
    test_stmt_err("INSERT INTO t VALUES (1)", error_code::sql_execution_exception); // sequence not found and DML should fail unexpectedly
    ASSERT_EQ(0, seq_count());
    {
        // even if sequence does not work, query should work
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t", result);
        ASSERT_EQ(1, result.size());
    }
    // verify drop completely clean-up and recreation is successful
    execute_statement("DROP TABLE t");
    execute_statement("CREATE TABLE t (c0 int)");
    ASSERT_EQ(1, seq_count());
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("DROP TABLE t");
}

TEST_F(transaction_fail_ddl_test, create_table_with_identity_column) {
    // similar as create_pkless_table, but using identity column instead of generated primary key
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("CREATE TABLE t (c0 int primary key, c1 int generated always as identity)", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    execute_statement("INSERT INTO t(c0) VALUES (1)"); // using in-memory sequence, this dml won't fail
    ASSERT_EQ(0, seq_count()); // no entry due to abort

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());  // warning message should be shown
    test_stmt_err("INSERT INTO t(c0) VALUES (2)", error_code::sql_execution_exception); // sequence not found and DML should fail unexpectedly
    ASSERT_EQ(0, seq_count());
    {
        // even if sequence does not work, query should work
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t", result);
        ASSERT_EQ(1, result.size());
    }
    // verify drop completely clean-up and recreation is successful
    execute_statement("DROP TABLE t");
    execute_statement("CREATE TABLE t (c0 int primary key, c1 int generated always as identity)");
    ASSERT_EQ(1, seq_count());
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (2)");
    execute_statement("DROP TABLE t");
}

TEST_F(transaction_fail_ddl_test, create_pkless_table_with_identity_column) {
    // same as create_pkless_table, but with identity column to add more generated column
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("CREATE TABLE t (c0 int, c1 int generated by default as identity)", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    execute_statement("INSERT INTO t(c0) VALUES (1)"); // using in-memory sequence, this dml won't fail
    ASSERT_EQ(0, seq_count());

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());  // warning message should be shown
    test_stmt_err("INSERT INTO t(c0) VALUES (2)", error_code::sql_execution_exception); // sequence not found and DML should fail unexpectedly
    ASSERT_EQ(0, seq_count());
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM t", result);
        ASSERT_EQ(1, result.size());
    }
    execute_statement("DROP TABLE t");
    execute_statement("CREATE TABLE t (c0 int, c1 int generated by default as identity)");
    ASSERT_EQ(2, seq_count());
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("DROP TABLE t");
}

TEST_F(transaction_fail_ddl_test, drop_simple_table) {
    // verify aborting transaction to drop table with primary key with no identity colum does no harm
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    execute_statement("CREATE TABLE t (c0 int primary key)");
    ASSERT_EQ(0, seq_count());
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (2)");
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("DROP TABLE t", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    ASSERT_EQ(0, seq_count());
    execute_statement("CREATE TABLE t (c0 int primary key)");
    ASSERT_EQ(0, seq_count());
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (2)");
    execute_statement("DROP TABLE t");
    ASSERT_EQ(0, seq_count());
}

TEST_F(transaction_fail_ddl_test, drop_pkless_table) {
    // verify aborting transaction to drop leaves sequence entry on the system table, but it does no harm (just leak)
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    execute_statement("CREATE TABLE t (c0 int)");
    ASSERT_EQ(1, seq_count());
    auto seqs = seq_list();
    ASSERT_TRUE(exists_seq(seqs[0]));
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (1)");
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("DROP TABLE t", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    ASSERT_EQ(1, seq_count()); // left entry due to abort
    ASSERT_TRUE(! exists_seq(seqs[0])); // though table entry is left, sequence is removed correctly
    execute_statement("CREATE TABLE t (c0 int)");
    ASSERT_EQ(2, seq_count());
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("DROP TABLE t");
    ASSERT_EQ(1, seq_count());
}

TEST_F(transaction_fail_ddl_test, drop_pkless_table_with_restart) {
    // same as drop_pkless_table, but with restart
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    execute_statement("CREATE TABLE t (c0 int)");
    ASSERT_EQ(1, seq_count());
    auto seqs = seq_list();
    ASSERT_TRUE(exists_seq(seqs[0]));
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (1)");
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("DROP TABLE t", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    ASSERT_EQ(1, seq_count()); // left entry due to abort
    ASSERT_TRUE(! exists_seq(seqs[0])); // though table entry is left, sequence is removed correctly

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());  // warning message should be shown

    execute_statement("CREATE TABLE t (c0 int)");
    ASSERT_EQ(2, seq_count());
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("INSERT INTO t VALUES (1)");
    execute_statement("DROP TABLE t");
    ASSERT_EQ(1, seq_count());
}

TEST_F(transaction_fail_ddl_test, drop_table_with_identity_column) {
    // same as drop_pkless_table, but with identity column
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity)");
    ASSERT_EQ(2, seq_count());
    auto seqs = seq_list();
    ASSERT_TRUE(exists_seq(seqs[0]));
    ASSERT_TRUE(exists_seq(seqs[1]));
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("DROP TABLE t", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    ASSERT_EQ(2, seq_count()); // left entry due to abort
    ASSERT_TRUE(! exists_seq(seqs[0]));
    ASSERT_TRUE(! exists_seq(seqs[1]));
    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity)");
    ASSERT_EQ(4, seq_count());
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("DROP TABLE t");
    ASSERT_EQ(2, seq_count());
}

TEST_F(transaction_fail_ddl_test, drop_table_with_identity_column_with_restart) {
    // same as drop_pkless_table_with_restart, but with identity column
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity)");
    ASSERT_EQ(2, seq_count());
    auto seqs = seq_list();
    ASSERT_TRUE(exists_seq(seqs[0]));
    ASSERT_TRUE(exists_seq(seqs[1]));
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("DROP TABLE t", *tx);
        ASSERT_EQ(status::ok, tx->abort());
    }
    ASSERT_EQ(2, seq_count()); // left entry due to abort
    ASSERT_TRUE(! exists_seq(seqs[0]));
    ASSERT_TRUE(! exists_seq(seqs[1]));

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());  // warning message should be shown

    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity)");
    ASSERT_EQ(4, seq_count());
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("DROP TABLE t");
    ASSERT_EQ(2, seq_count());
}

TEST_F(transaction_fail_ddl_test, drop_table_missing_sequence_entry) {
    // simulate the situation that sequences system table entry is some how missing
    // without db restrt, this works as in-memory object is used
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity)");
    ASSERT_EQ(2, seq_count());
    auto seqs = seq_list();
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    {
        // simulate system table entry is missing
        auto tx = utils::create_transaction(*db_);
        auto tctx = get_transaction_context(*tx);
        executor::sequence::metadata_store ms{*tctx->object()};
        ms.remove(0);
        ms.remove(1);
        ASSERT_EQ(status::ok, tx->commit());
    }
    ASSERT_EQ(0, seq_count());
    EXPECT_TRUE(exists_seq(seqs[0]));
    EXPECT_TRUE(exists_seq(seqs[1]));
    execute_statement("DROP TABLE t"); // this happens to work using in-memory object (sequences_)
    ASSERT_EQ(0, seq_count());
    EXPECT_TRUE(! exists_seq(seqs[0]));
    EXPECT_TRUE(! exists_seq(seqs[1]));
    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity)");
    ASSERT_EQ(2, seq_count());
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("DROP TABLE t");
    ASSERT_EQ(0, seq_count());
}

TEST_F(transaction_fail_ddl_test, drop_table_missing_sequence_entry_with_restart) {
    // same as drop_table_missing_sequence_entry, but with restart
    // with restart, drop fails to clean up the sharksfin sequence because there is no seq id available and it leaks
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity)");
    ASSERT_EQ(2, seq_count());
    auto seqs = seq_list();
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    {
        // simulate system table entry is missing
        auto tx = utils::create_transaction(*db_);
        auto tctx = get_transaction_context(*tx);
        executor::sequence::metadata_store ms{*tctx->object()};
        ms.remove(0);
        ms.remove(1);
        ASSERT_EQ(status::ok, tx->commit());
    }

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());  // warning message should be shown

    ASSERT_EQ(0, seq_count());
    EXPECT_TRUE(exists_seq(seqs[0]));
    EXPECT_TRUE(exists_seq(seqs[1]));
    execute_statement("DROP TABLE t");
    ASSERT_EQ(0, seq_count());
    EXPECT_TRUE(exists_seq(seqs[0])); // fail to clean up
    EXPECT_TRUE(exists_seq(seqs[1])); // fail to clean up
    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity)");
    ASSERT_EQ(2, seq_count());
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("DROP TABLE t");
    ASSERT_EQ(0, seq_count());
}

TEST_F(transaction_fail_ddl_test, drop_table_missing_sequence_with_restart) {
    // verify drop table does clean up even if some of the sharksfin sequence is missing
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback by abort";
    }
    api::transaction_option opts{};
    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity, c2 int generated always as identity, c3 int generated always as identity)");
    ASSERT_EQ(4, seq_count());
    auto seqs = seq_list();
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    {
        // simulate part of the sequence is missing somehow
        EXPECT_TRUE(exists_seq(seqs[2]));
        EXPECT_TRUE(remove_seq(seqs[2]));
    }

    ASSERT_EQ(status::ok, db_->stop());
    ASSERT_EQ(status::ok, db_->start());

    execute_statement("DROP TABLE t");
    ASSERT_EQ(0, seq_count());
    EXPECT_TRUE(! exists_seq(seqs[0]));
    EXPECT_TRUE(! exists_seq(seqs[1]));
    EXPECT_TRUE(! exists_seq(seqs[2]));
    EXPECT_TRUE(! exists_seq(seqs[3]));

    execute_statement("CREATE TABLE t (c0 int, c1 int generated always as identity, c2 int generated always as identity, c3 int generated always as identity)");
    ASSERT_EQ(4, seq_count());
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("INSERT INTO t(c0) VALUES (1)");
    execute_statement("DROP TABLE t");
    ASSERT_EQ(0, seq_count());
}

}
