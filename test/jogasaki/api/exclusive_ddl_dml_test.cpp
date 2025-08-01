/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <future>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>
#include <xmmintrin.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_tx.h>

#include "../api/api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class exclusive_ddl_dml_test :
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
};

using namespace std::string_view_literals;

TEST_F(exclusive_ddl_dml_test, starting_dml_blocked_by_create_table_tx) {
    {
        auto tx0 = utils::create_transaction(*db_);
        execute_statement("CREATE TABLE t (c0 int primary key)", *tx0);
        auto tx1 = utils::create_transaction(*db_);
        test_stmt_err("select * from t", *tx1, error_code::sql_execution_exception);
        // verify tx abort by the error above
        test_stmt_err("select * from t", *tx1, error_code::inactive_transaction_exception);
        ASSERT_EQ(status::ok, tx0->commit());
    }
    std::vector<mock::basic_record> result{};
    execute_query("select * from t", result);
    ASSERT_EQ(result.size(), 0);
}

TEST_F(exclusive_ddl_dml_test, repeat_starting_dml_blocked_by_create_table_tx) {
    // regression testcase reported in #1230 - on the 2nd trial dml did not get blocked by ddl
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("CREATE TABLE t (c0 int primary key)", *tx);
        test_stmt_err("select * from t", error_code::sql_execution_exception);
        ASSERT_EQ(status::ok, tx->commit());
    }
    std::vector<mock::basic_record> result{};
    execute_query("select * from t", result);
    ASSERT_EQ(result.size(), 0);
    execute_statement("DROP TABLE t");
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("CREATE TABLE t (c0 int primary key)", *tx);
        test_stmt_err("select * from t", error_code::sql_execution_exception);  // this was not blocked somehow
        ASSERT_EQ(status::ok, tx->commit());
    }
}

TEST_F(exclusive_ddl_dml_test, ddl_and_dml_in_same_tx) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    auto tx = utils::create_transaction(*db_);
    execute_statement("CREATE TABLE t (c0 int)", *tx);
    execute_statement("insert into t values (0)", *tx);
    execute_statement("insert into t values (1)", *tx);
    execute_statement("insert into t values (2)", *tx);
    std::vector<mock::basic_record> result{};
    execute_query("select count(*) from t", *tx, result);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ((mock::create_nullable_record<kind::int8>(3)), result[0]);
    // currently drop table here crashes tx engine. TODO Delay deleting storage after tx end.
    // execute_statement("DROP TABLE t", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(exclusive_ddl_dml_test, starting_create_table_blocked_by_dml_req) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t0 (c0 int primary key)");
    execute_statement("CREATE TABLE t1 (c0 int primary key)");
    execute_statement("INSERT INTO t0 values (1)");
    std::size_t t0_cnt = 1;
    std::size_t t1_cnt = 0;
    for(std::size_t i=0; i < 7; ++i) { // choose count to make the query long enough (e.g. 100ms) to run ddl while query is on-going
        execute_statement("INSERT INTO t1 SELECT c0+"+std::to_string(t1_cnt)+" FROM t0");
        t1_cnt += t0_cnt;
        execute_statement("INSERT INTO t0 SELECT c0+"+std::to_string(t0_cnt)+" FROM t1");
        t0_cnt += t1_cnt;
    }

    std::cerr << "number of rows in t0:" << t0_cnt << std::endl;
    auto& smgr = *global::storage_manager();
    auto s = smgr.find_by_name("t0");
    ASSERT_TRUE(s.has_value());
    auto c = smgr.find_entry(s.value());
    ASSERT_TRUE(c);

    auto f = std::async(std::launch::async, [&, this]() {
        std::vector<mock::basic_record> result{};
        execute_query("select count(*) from t0", result);
    });
    while(c->can_lock()) { _mm_pause(); }  // wait for the query to acquire shared lock
    {
        auto tx = utils::create_transaction(*db_);
        test_stmt_err("drop table t0", *tx, error_code::sql_execution_exception, "DDL operation was blocked by other DML operation");
        // verify tx abort by the error above
        test_stmt_err("drop table t0", *tx, error_code::inactive_transaction_exception);
    }
    f.get();
}

TEST_F(exclusive_ddl_dml_test, dml_error_after_drop) {
    // verify that dml after drop table does not cause crash
    // after dropping table, the entry is gone and lock is not held any more, so the error message is different
    execute_statement("CREATE TABLE t (c0 int primary key)");
    {
        auto tx0 = utils::create_transaction(*db_);
        execute_statement("drop table t", *tx0);
        test_stmt_err("select * from t", error_code::symbol_analyze_exception);
        ASSERT_EQ(status::ok, tx0->commit());
    }
    test_stmt_err("select * from t", error_code::symbol_analyze_exception);
}

TEST_F(exclusive_ddl_dml_test, starting_dml_blocked_by_create_index_tx) {
    execute_statement("CREATE TABLE t (c0 int)");
    {
        auto tx0 = utils::create_transaction(*db_);
        execute_statement("CREATE INDEX i on t (c0)", *tx0);
        auto tx1 = utils::create_transaction(*db_);
        test_stmt_err("select * from t", *tx1, error_code::sql_execution_exception);
        // verify tx abort by the error above
        test_stmt_err("select * from t", *tx1, error_code::inactive_transaction_exception);
        ASSERT_EQ(status::ok, tx0->commit());
    }
    std::vector<mock::basic_record> result{};
    execute_query("select * from t", result);
    ASSERT_EQ(result.size(), 0);
}

TEST_F(exclusive_ddl_dml_test, starting_dml_blocked_by_drop_index_tx) {
    execute_statement("CREATE TABLE t (c0 int)");
    execute_statement("CREATE INDEX i on t (c0)");
    {
        auto tx0 = utils::create_transaction(*db_);
        execute_statement("DROP INDEX i", *tx0);
        auto tx1 = utils::create_transaction(*db_);
        test_stmt_err("select * from t", *tx1, error_code::sql_execution_exception);
        // verify tx abort by the error above
        test_stmt_err("select * from t", *tx1, error_code::inactive_transaction_exception);
        ASSERT_EQ(status::ok, tx0->commit());
    }
    std::vector<mock::basic_record> result{};
    execute_query("select * from t", result);
    ASSERT_EQ(result.size(), 0);
}

TEST_F(exclusive_ddl_dml_test, create_table_and_create_index_in_same_tx) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    auto tx = utils::create_transaction(*db_);
    execute_statement("CREATE TABLE t (c0 int)", *tx);
    execute_statement("CREATE INDEX i on t (c0)", *tx);
    execute_statement("insert into t values (0)", *tx);
    execute_statement("insert into t values (1)", *tx);
    execute_statement("insert into t values (2)", *tx);
    std::vector<mock::basic_record> result{};
    execute_query("select count(*) from t", *tx, result);
    ASSERT_EQ(result.size(), 1);
    EXPECT_EQ((mock::create_nullable_record<kind::int8>(3)), result[0]);
    // currently drop table here crashes tx engine. TODO Delay deleting storage after tx end.
    // execute_statement("DROP TABLE t", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(exclusive_ddl_dml_test, starting_create_or_drop_index_blocked_by_dml_req) {
    // same as starting_create_table_blocked_by_dml_req, except for creating/dropping index
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t0 (c0 int primary key)");
    execute_statement("CREATE INDEX i0 on t0 (c0)");
    execute_statement("CREATE TABLE t1 (c0 int primary key)");
    execute_statement("INSERT INTO t0 values (1)");
    std::size_t t0_cnt = 1;
    std::size_t t1_cnt = 0;
    for(std::size_t i=0; i < 7; ++i) { // choose count to make the query long enough (e.g. 100ms) to run ddl while query is on-going
        execute_statement("INSERT INTO t1 SELECT c0+"+std::to_string(t1_cnt)+" FROM t0");
        t1_cnt += t0_cnt;
        execute_statement("INSERT INTO t0 SELECT c0+"+std::to_string(t0_cnt)+" FROM t1");
        t0_cnt += t1_cnt;
    }

    std::cerr << "number of rows in t0:" << t0_cnt << std::endl;
    auto& smgr = *global::storage_manager();
    auto s = smgr.find_by_name("t0");
    ASSERT_TRUE(s.has_value());
    auto c = smgr.find_entry(s.value());
    ASSERT_TRUE(c);

    auto f = std::async(std::launch::async, [&, this]() {
        std::vector<mock::basic_record> result{};
        execute_query("select count(*) from t0", result);
    });
    while(c->can_lock()) { _mm_pause(); }  // wait for the query to acquire shared lock
    {
        // drop index is blocked by DML
        auto tx = utils::create_transaction(*db_);
        test_stmt_err("drop index i0", *tx, error_code::sql_execution_exception, "DDL operation was blocked by other DML operation");
        // verify tx abort by the error above
        test_stmt_err("drop index i0", *tx, error_code::inactive_transaction_exception);
    }

    // currently creating index with data is not allowed, so this test cannot be run
    /*
    {
        // create index is blocked by DML
        auto tx = utils::create_transaction(*db_);
        test_stmt_err("create index i1 on t0 (c0)", *tx, error_code::sql_execution_exception, "DDL operation was blocked by other DML operation");
        // verify tx abort by the error above
        test_stmt_err("create index i1 on t0 (c0)", *tx, error_code::sql_execution_exception);
    }
    */
    f.get();
}

}
