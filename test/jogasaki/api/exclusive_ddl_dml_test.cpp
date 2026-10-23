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

#include <atomic>
#include <future>
#include <map>
#include <memory>
#include <mutex>
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
#include <jogasaki/error/error_info.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/maintenance_storage.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/finally.h>
#include <jogasaki/utils/runner.h>

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

TEST_F(exclusive_ddl_dml_test, starting_ddls_blocked_by_dml_req) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t0 (c0 int primary key)");
    execute_statement("CREATE TABLE t1 (c0 int primary key)");
    execute_statement("INSERT INTO t0 values (1)");
    std::size_t t0_cnt = 1;
    std::size_t t1_cnt = 0;
    for(std::size_t i=0; i < 10; ++i) { // choose count to make the query long enough (e.g. 100ms) to run ddl while query is on-going
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
        test_stmt_err("drop table t0", *tx, error_code::sql_execution_exception, "DDL operation was blocked by other DML operation. table:\"t0\"");
        // verify tx abort by the error above
        test_stmt_err("drop table t0", *tx, error_code::inactive_transaction_exception);
    }
    {
        auto tx = utils::create_transaction(*db_);
        test_stmt_err("grant select on table t0 to public", *tx, error_code::sql_execution_exception, "DDL operation was blocked by other DML operation. table:\"t0\"");
        // verify tx abort by the error above
        test_stmt_err("grant select on table t0 to public", *tx, error_code::inactive_transaction_exception);
    }
    {
        auto tx = utils::create_transaction(*db_);
        test_stmt_err("revoke select on table t0 from public", *tx, error_code::sql_execution_exception, "DDL operation was blocked by other DML operation. table:\"t0\"");
        // verify tx abort by the error above
        test_stmt_err("revoke select on table t0 from public", *tx, error_code::inactive_transaction_exception);
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

TEST_F(exclusive_ddl_dml_test, drop_table_blocked_by_create_index_tx) {
    execute_statement("CREATE TABLE t (c0 int)");
    {
        auto tx0 = utils::create_transaction(*db_);
        execute_statement("CREATE INDEX i on t (c0)", *tx0);
        auto tx1 = utils::create_transaction(*db_);
        test_stmt_err("drop table t", *tx1, error_code::sql_execution_exception,
            "DDL operation was blocked by other DML operation. table:\"t\""); //TODO fix error msg
        // verify tx abort by the error above
        test_stmt_err("drop table t", *tx1, error_code::inactive_transaction_exception);
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
    execute_statement("grant select,insert on table t to public", *tx);
    execute_statement("revoke insert on table t from public", *tx);
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
        test_stmt_err("drop index i0", *tx, error_code::sql_execution_exception, "DDL operation was blocked by other DML operation. table:\"t0\"");
        // verify tx abort by the error above
        test_stmt_err("drop index i0", *tx, error_code::inactive_transaction_exception);
    }

    // currently creating index with data is not allowed, so this test cannot be run
    /*
    {
        // create index is blocked by DML
        auto tx = utils::create_transaction(*db_);
        test_stmt_err("create index i1 on t0 (c0)", *tx, error_code::sql_execution_exception, "DDL operation was blocked by other DML operation. table:\"t0\"");
        // verify tx abort by the error above
        test_stmt_err("create index i1 on t0 (c0)", *tx, error_code::sql_execution_exception);
    }
    */
    f.get();
}

TEST_F(exclusive_ddl_dml_test, starting_dml_blocked_by_grant_table_tx) {
    execute_statement("CREATE TABLE t (c0 int primary key)");
    {
        auto tx0 = utils::create_transaction(*db_);
        execute_statement("grant select on table t to public", *tx0);
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

TEST_F(exclusive_ddl_dml_test, starting_dml_blocked_by_revoke_table_tx) {
    execute_statement("CREATE TABLE t (c0 int primary key)");
    {
        auto tx0 = utils::create_transaction(*db_);
        execute_statement("revoke select on table t from public", *tx0);
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

TEST_F(exclusive_ddl_dml_test, starting_dml_blocked_by_truncate_table_tx) {
    execute_statement("CREATE TABLE t (c0 int primary key)");
    execute_statement("INSERT INTO t values (1)");
    {
        auto tx0 = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t", *tx0);
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

TEST_F(exclusive_ddl_dml_test, starting_truncate_blocked_by_dml_req) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t0 (c0 int primary key)");
    execute_statement("CREATE TABLE t1 (c0 int primary key)");
    execute_statement("INSERT INTO t0 values (1)");
    std::size_t t0_cnt = 1;
    std::size_t t1_cnt = 0;
    for(std::size_t i=0; i < 10; ++i) { // choose count to make the query long enough (e.g. 100ms) to run ddl while query is on-going
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
        test_stmt_err("TRUNCATE TABLE t0", *tx, error_code::sql_execution_exception, "DDL operation was blocked by other DML operation. table:\"t0\"");
        // verify tx abort by the error above
        test_stmt_err("TRUNCATE TABLE t0", *tx, error_code::inactive_transaction_exception);
    }
    f.get();
}

TEST_F(exclusive_ddl_dml_test, dml_compiled_before_drop_blocked_after_commit) {
    // regression test: DML compiled before DROP TABLE and executed after the DROP committed used to lock
    // the dropped storage entry successfully and crash by accessing missing storage
    execute_statement("CREATE TABLE t (c0 int primary key)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO t VALUES (1)", stmt));
    execute_statement("DROP TABLE t");
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::err_illegal_operation, tx->execute(*stmt));
}

TEST_F(exclusive_ddl_dml_test, dml_compiled_before_truncate_blocked_after_commit) {
    // regression test: DML compiled before TRUNCATE TABLE and executed after the TRUNCATE committed used to lock
    // the old storage entry and access the new storage without locking it
    execute_statement("CREATE TABLE t (c0 int primary key, c1 int)");
    execute_statement("CREATE INDEX i ON t (c1)");
    execute_statement("INSERT INTO t VALUES (0, 0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO t VALUES (1, 1)", stmt));
    execute_statement("TRUNCATE TABLE t");
    {
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::err_illegal_operation, tx->execute(*stmt));
    }
    // newly compiled statement runs successfully
    execute_statement("INSERT INTO t VALUES (2, 2)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t WHERE c1 = 2", result);
    ASSERT_EQ(1, result.size());
}

TEST_F(exclusive_ddl_dml_test, dml_compiled_before_drop_in_same_tx) {
    // DML compiled before DROP TABLE in the same transaction used to run without storage and fail by internal error
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t (c0 int primary key, c1 int)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO t VALUES (1, 1)", stmt));
    auto tx = utils::create_transaction(*db_);
    execute_statement("DROP TABLE t", *tx);
    ASSERT_EQ(status::err_illegal_operation, tx->execute(*stmt));
}

TEST_F(exclusive_ddl_dml_test, dml_compiled_before_truncate_in_same_tx) {
    // DML compiled before TRUNCATE TABLE refers to the old storage entry, so it's blocked even in the same transaction
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t (c0 int primary key, c1 int)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO t VALUES (1, 1)", stmt));
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t", *tx);
        ASSERT_EQ(status::err_illegal_operation, tx->execute(*stmt));
    }
    {
        // statement compiled after TRUNCATE runs successfully in the same transaction
        auto tx = utils::create_transaction(*db_);
        execute_statement("TRUNCATE TABLE t", *tx);
        execute_statement("INSERT INTO t VALUES (2, 2)", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(2, 2)), result[0]);
}

TEST_F(exclusive_ddl_dml_test, dml_compiled_before_drop_and_create_in_same_tx) {
    // DML compiled before DROP TABLE and CREATE TABLE with the same name in the same transaction used to write
    // records with the old table layout into the new table, which caused data corruption
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t (c0 int primary key, c1 int)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO t VALUES (1, 1)", stmt));
    {
        auto tx = utils::create_transaction(*db_);
        execute_statement("DROP TABLE t", *tx);
        execute_statement("CREATE TABLE t (c0 varchar(10) primary key)", *tx);
        ASSERT_EQ(status::err_illegal_operation, tx->execute(*stmt));
    }
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(exclusive_ddl_dml_test, resolve_statement_prepared_before_drop) {
    // resolving the statement prepared before DROP TABLE used to throw an exception out of the api
    // because the table referenced by the prepared statement was removed
    execute_statement("CREATE TABLE t (c0 int primary key, c1 int)");
    execute_statement("CREATE INDEX i ON t (c1)");
    api::statement_handle prepared{};
    std::unordered_map<std::string, api::field_type_kind> variables{{"p0", api::field_type_kind::int4}};
    ASSERT_EQ(status::ok, db_->prepare("INSERT INTO t VALUES (:p0, :p0)", variables, prepared));
    execute_statement("DROP TABLE t");
    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_compiler_error, db_->resolve(prepared, std::shared_ptr{std::move(ps)}, stmt));
    ASSERT_EQ(status::ok, db_->destroy_statement(prepared));
}

TEST_F(exclusive_ddl_dml_test, dml_compile_fails_if_table_storage_entry_is_missing) {
    // DML compiled when the table definition exists but its storage entry is not found by name (e.g. DROP TABLE
    // removed the entry after the definition was read, or the metadata is inconsistent) used to have no storage
    // to lock and run without locking the table. Now it fails as the table is not found.
    execute_statement("CREATE TABLE t (c0 int primary key)");
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name("t");
    ASSERT_TRUE(e);
    // emulate the situation by removing the name from the storage entry
    ASSERT_TRUE(smgr.reserve_delete_entry(e.value()));
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::err_not_found, db_->create_executable("INSERT INTO t VALUES (1)", stmt));
}

TEST_F(exclusive_ddl_dml_test, truncate_while_dml_requests_are_compiled_concurrently) {
    // regression test: DML compiled while TRUNCATE TABLE is replacing the storage entry used to lock the new entry
    // before TRUNCATE locked it, and TRUNCATE failed leaving broken storage entries, which crashed subsequent DML.
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t (c0 bigint primary key, c1 bigint)");
    execute_statement("CREATE INDEX i ON t (c1)");
    auto& smgr = *global::storage_manager();
    std::atomic_bool stop{false};
    std::atomic_long seq{0};
    std::atomic_size_t internal_errors{0};
    auto worker = [&]() {
        while(! stop) {
            auto v = std::to_string(seq++);
            // compile every time so that it may run concurrently with TRUNCATE
            std::unique_ptr<api::executable_statement> stmt{};
            if(db_->create_executable("INSERT INTO t VALUES (" + v + "," + v + ")", stmt) != status::ok) {
                continue;
            }
            auto tx = utils::create_transaction(*db_);
            if(auto res = tx->execute(*stmt); res != status::ok) {
                // DML blocked by TRUNCATE is normal, but internal error means accessing missing storage
                if(res == status::err_unknown) {
                    ++internal_errors;
                }
                tx->abort();
                continue;
            }
            (void) tx->commit();
        }
    };
    std::vector<std::future<void>> workers{};
    // stop workers even when assertion fails, otherwise destructor of the futures waits for them forever
    utils::finally stop_workers{[&]() {
        stop = true;
    }};
    for(std::size_t i = 0; i < 4; ++i) {
        workers.emplace_back(std::async(std::launch::async, worker));
    }
    std::size_t succeeded = 0;
    std::size_t blocked = 0;
    for(std::size_t i = 0; i < 50; ++i) {
        auto table_key = smgr.get_storage_key("t");
        auto index_key = smgr.get_storage_key("i");
        ASSERT_TRUE(table_key);
        ASSERT_TRUE(index_key);

        auto tx = utils::create_transaction(*db_);
        status res{};
        std::shared_ptr<error::error_info> err{};
        (void) builder().text("TRUNCATE TABLE t").tx(*tx).st(res).error(err).expect_error(true).run();
        if(res != status::ok) {
            // the only acceptable failure is the one blocked by on-going DML on locking the table first,
            // and the storages must be left untouched
            ASSERT_EQ(status::err_illegal_operation, res);
            ASSERT_TRUE(err);
            ASSERT_EQ(error_code::sql_execution_exception, err->code());
            ASSERT_EQ("DDL operation was blocked by other DML operation. table:\"t\"", err->message());
            ASSERT_EQ(table_key, smgr.get_storage_key("t"));
            ASSERT_EQ(index_key, smgr.get_storage_key("i"));
            ++blocked;
            continue;
        }
        ASSERT_EQ(status::ok, tx->commit());
        // verify both table and index storages are replaced with new ones
        auto new_table_key = smgr.get_storage_key("t");
        auto new_index_key = smgr.get_storage_key("i");
        ASSERT_TRUE(new_table_key);
        ASSERT_TRUE(new_index_key);
        ASSERT_NE(table_key, new_table_key);
        ASSERT_NE(index_key, new_index_key);
        ++succeeded;
    }
    stop = true;
    for(auto&& f : workers) {
        f.get();
    }
    std::cerr << "truncate succeeded:" << succeeded << " blocked:" << blocked << std::endl;
    ASSERT_LT(0, succeeded);
    ASSERT_EQ(0, internal_errors);
    execute_statement("INSERT INTO t VALUES (-1, -1)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t WHERE c1 = -1", result);
    ASSERT_EQ(1, result.size());
}

TEST_F(exclusive_ddl_dml_test, truncate_while_dml_compiled_concurrently_sees_consistent_definitions) {
    // regression test for tsurugi-issues #1587: TRUNCATE TABLE used to re-register the table and index definitions
    // into the provider one by one, and DML compiled concurrently (without locks) saw the intermediate state, e.g.
    // the new table object with the old index objects. The compilation failed with primary_index_not_found or
    // an unexpected compile error, or could produce a plan that misses writing the secondary index.
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t (c0 bigint primary key, c1 bigint)");
    execute_statement("CREATE INDEX i ON t (c1)");
    auto index_query = [](std::string_view c1) {
        return "SELECT c0 FROM t WHERE c1 = " + std::string{c1};
    };
    {
        std::string plan{};
        explain_statement(index_query("0"), plan);
        ASSERT_NE(std::string::npos, plan.find("\"i\"")) << plan;  // verify the query reads the secondary index
    }
    std::atomic_bool stop{false};
    std::atomic_long seq{0};
    std::atomic_size_t inserted{0};
    std::atomic_size_t blocked_dml{0};
    std::mutex mtx{};
    std::map<std::string, std::size_t> compile_errors{};  // error code and message -> count
    std::map<status, std::size_t> exec_errors{};  // unexpected execution errors
    auto worker = [&]() {
        while(! stop) {
            auto v = std::to_string(seq++);
            // compile every time without placeholders so that it may run concurrently with TRUNCATE
            std::unique_ptr<api::executable_statement> stmt{};
            std::shared_ptr<error::error_info> err{};
            if(auto rc = db_impl()->create_executable("INSERT INTO t VALUES (" + v + "," + v + ")", stmt, err);
               rc != status::ok) {
                // aggregate by the message without the sql text
                std::string msg = err ? std::string{err->message()} : std::string{to_string_view(rc)};
                msg = msg.substr(0, msg.find(" sql:"));
                std::lock_guard lk{mtx};
                ++compile_errors[std::string{to_string_view(err ? err->code() : error_code::none)} + " " + msg];
                continue;
            }
            auto tx = utils::create_transaction(*db_);
            if(auto res = tx->execute(*stmt); res != status::ok) {
                if(res == status::err_illegal_operation) {
                    // DML blocked by TRUNCATE is normal
                    ++blocked_dml;
                } else {
                    std::lock_guard lk{mtx};
                    ++exec_errors[res];
                }
                tx->abort();
                continue;
            }
            if(tx->commit() == status::ok) {
                ++inserted;
            }
        }
    };
    std::vector<std::future<void>> workers{};
    // stop workers even when assertion fails, otherwise destructor of the futures waits for them forever
    utils::finally stop_workers{[&]() {
        stop = true;
    }};
    for(std::size_t i = 0; i < 4; ++i) {
        workers.emplace_back(std::async(std::launch::async, worker));
    }
    std::size_t truncated = 0;
    std::size_t inconsistent = 0;
    for(std::size_t i = 0; i < 20; ++i) {
        {
            // verify every row in the primary index is found via the secondary index, reading them in the same
            // snapshot (c0 = c1 for all rows)
            auto rtx = utils::create_transaction(*db_, true, false);
            std::vector<mock::basic_record> primary{};
            (void) builder().text("SELECT c0 FROM t").tx(*rtx).output_records(primary).run();
            for(auto&& r : primary) {
                auto v = std::to_string(r.get_value<std::int64_t>(0));
                std::vector<mock::basic_record> secondary{};
                (void) builder().text(index_query(v)).tx(*rtx).output_records(secondary).run();
                if(secondary.size() != 1) {
                    ++inconsistent;
                    std::cerr << "row c0=" << v << " is not found via the secondary index" << std::endl;
                }
            }
            ASSERT_EQ(status::ok, rtx->commit());
        }
        auto tx = utils::create_transaction(*db_);
        status res{};
        std::shared_ptr<error::error_info> err{};
        (void) builder().text("TRUNCATE TABLE t").tx(*tx).st(res).error(err).expect_error(true).run();
        if(res != status::ok) {
            // blocked by on-going DML on locking the table first
            ASSERT_EQ(status::err_illegal_operation, res);
            continue;
        }
        ASSERT_EQ(status::ok, tx->commit());
        ++truncated;
    }
    stop = true;
    for(auto&& f : workers) {
        f.get();
    }
    std::cerr << "truncated:" << truncated << " inserted:" << inserted << " blocked dml:" << blocked_dml << std::endl;
    for(auto&& [msg, cnt] : compile_errors) {
        std::cerr << "compile error x" << cnt << ": " << msg << std::endl;
    }
    for(auto&& [st, cnt] : exec_errors) {
        std::cerr << "execution error x" << cnt << ": " << st << std::endl;
    }
    EXPECT_LT(0, truncated);
    EXPECT_LT(0, inserted);
    EXPECT_TRUE(compile_errors.empty());
    EXPECT_TRUE(exec_errors.empty());
    EXPECT_EQ(0, inconsistent);
}

TEST_F(exclusive_ddl_dml_test, create_index_fails_if_table_storage_entry_is_missing) {
    // CREATE INDEX used to check the table is empty before locking the table, and crashed when the table name was
    // missing (e.g. DROP TABLE removed the storage entry concurrently)
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t (c0 int primary key, c1 int)");
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name("t");
    ASSERT_TRUE(e);
    // emulate the situation by removing the name from the storage entry
    ASSERT_TRUE(smgr.reserve_delete_entry(e.value()));
    test_stmt_err("CREATE INDEX i ON t (c1)", error_code::target_not_found_exception);
}

static void test_table_area_keeps_storage(
    api_test_base& t,
    std::string_view ddl,
    std::vector<std::string> const& write_preserves,
    std::vector<std::string> const& read_areas
) {
    // the storage of a table specified as write preserve or read area of LTX used to be deleted by the
    // maintenance thread when the table was dropped/truncated, and the cc engine accessed the deleted storage
    // on the end of the LTX
    t.execute_statement("CREATE TABLE t (c0 int primary key)");
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name("t");
    ASSERT_TRUE(e);
    auto tx = utils::create_transaction(*t.db_, false, true, write_preserves, read_areas);
    ASSERT_TRUE(tx);
    t.execute_statement(ddl);  // by other transaction
    (void) storage::maintenance_storage(nullptr);
    EXPECT_TRUE(smgr.find_entry(e.value()));  // still referenced by the LTX
    ASSERT_EQ(status::ok, tx->abort());
    (void) storage::maintenance_storage(nullptr);
    EXPECT_FALSE(smgr.find_entry(e.value()));
}

TEST_F(exclusive_ddl_dml_test, ltx_write_preserve_keeps_storage_of_dropped_table) {
    test_table_area_keeps_storage(*this, "DROP TABLE t", {"t"}, {});
}

TEST_F(exclusive_ddl_dml_test, ltx_write_preserve_keeps_storage_of_truncated_table) {
    test_table_area_keeps_storage(*this, "TRUNCATE TABLE t", {"t"}, {});
}

TEST_F(exclusive_ddl_dml_test, ltx_read_area_keeps_storage_of_dropped_table) {
    test_table_area_keeps_storage(*this, "DROP TABLE t", {}, {"t"});
}

static void test_ddl_while_creating_transaction(
    api_test_base& t,
    std::vector<std::string> const& ddls,
    std::vector<std::string> const& write_preserves,
    std::vector<std::string> const& read_areas_inclusive,
    std::vector<std::string> const& read_areas_exclusive
) {
    // The kvs transaction used to resolve the names of the write preserves and read areas again after the storage
    // references were added. If DDL re-pointed the name in between (e.g. TRUNCATE replacing the entry A with B),
    // the kvs transaction used the storage of B that had no reference, and it could be deleted by the maintenance
    // thread while the transaction used it. Now the kvs transaction uses the storage of the referenced entry A.
    t.execute_statement("CREATE TABLE t (c0 int primary key)");
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name("t");
    ASSERT_TRUE(e);

    // block the first transaction creation in the hook and run DDL meanwhile
    std::atomic_bool armed{true};
    std::promise<void> reached{};
    std::promise<void> resume{};
    auto resume_future = resume.get_future().share();
    executor::create_transaction_hook_for_testing([&, resume_future]() {
        if(! armed.exchange(false)) {
            // transactions used by DDL below
            return;
        }
        reached.set_value();
        resume_future.wait();
    });
    auto hook_cleanup = utils::finally([]() {
        executor::create_transaction_hook_for_testing({});
    });
    auto creating = std::async(std::launch::async, [&]() {
        api::transaction_option opt{
            transaction_type_kind::ltx, write_preserves, {}, read_areas_inclusive, read_areas_exclusive
        };
        return utils::create_transaction(*t.db_, std::move(opt));
    });
    reached.get_future().wait();
    std::optional<storage::storage_entry> replaced{};
    for(auto&& ddl : ddls) {
        t.execute_statement(ddl);  // by other transaction
        if(auto cur = smgr.find_by_name("t"); cur && cur != e) {
            replaced = cur;
        }
    }
    // the storage of the entry the name refers to now (if any) is deleted since nobody refers to it
    (void) storage::maintenance_storage(nullptr);
    resume.set_value();
    auto tx = creating.get();
    ASSERT_TRUE(tx);

    // the storage of the referenced entry is kept while the transaction is alive
    (void) storage::maintenance_storage(nullptr);
    EXPECT_TRUE(smgr.find_entry(e.value()));
    auto ctrl = smgr.find_entry(e.value());
    ASSERT_TRUE(ctrl);
    EXPECT_EQ(1, ctrl->ref_transaction_count());
    ASSERT_EQ(status::ok, tx->abort());
    (void) storage::maintenance_storage(nullptr);
    EXPECT_FALSE(smgr.find_entry(e.value()));
    if(replaced && ! smgr.find_by_name("t")) {
        // the replacing entry is dropped too
        EXPECT_FALSE(smgr.find_entry(replaced.value()));
    }
}

TEST_F(exclusive_ddl_dml_test, create_transaction_uses_referenced_storage_of_truncated_write_preserve) {
    test_ddl_while_creating_transaction(*this, {"TRUNCATE TABLE t"}, {"t"}, {}, {});
}

TEST_F(exclusive_ddl_dml_test, create_transaction_uses_referenced_storage_of_truncated_and_dropped_write_preserve) {
    test_ddl_while_creating_transaction(*this, {"TRUNCATE TABLE t", "DROP TABLE t"}, {"t"}, {}, {});
}

TEST_F(exclusive_ddl_dml_test, create_transaction_uses_referenced_storage_of_truncated_and_dropped_read_area_inclusive) {
    test_ddl_while_creating_transaction(*this, {"TRUNCATE TABLE t", "DROP TABLE t"}, {}, {"t"}, {});
}

TEST_F(exclusive_ddl_dml_test, create_transaction_uses_referenced_storage_of_truncated_and_dropped_read_area_exclusive) {
    test_ddl_while_creating_transaction(*this, {"TRUNCATE TABLE t", "DROP TABLE t"}, {}, {}, {"t"});
}

TEST_F(exclusive_ddl_dml_test, create_transaction_uses_referenced_storage_of_recreated_write_preserve) {
    test_ddl_while_creating_transaction(*this, {"DROP TABLE t", "CREATE TABLE t (c0 int primary key)"}, {"t"}, {}, {});
}

TEST_F(exclusive_ddl_dml_test, ltx_write_preserve_keeps_storage_of_dropped_index) {
    // the storage of a secondary index of a table specified as write preserve of LTX used to be deleted by the
    // maintenance thread when the index was dropped, and the cc engine accessed the deleted storage on the end of
    // the LTX
    execute_statement("CREATE TABLE t (c0 int primary key, c1 int)");
    execute_statement("CREATE INDEX i ON t (c1)");
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name("i");
    ASSERT_TRUE(e);
    auto tx = utils::create_transaction(*db_, false, true, {"t"});
    ASSERT_TRUE(tx);
    execute_statement("DROP INDEX i");  // by other transaction
    (void) storage::maintenance_storage(nullptr);
    EXPECT_TRUE(smgr.find_entry(e.value()));  // still referenced by the LTX through the primary entry
    ASSERT_EQ(status::ok, tx->abort());
    (void) storage::maintenance_storage(nullptr);
    EXPECT_FALSE(smgr.find_entry(e.value()));
}

TEST_F(exclusive_ddl_dml_test, create_table_with_name_of_existing_index) {
    // regression test: CREATE TABLE with the name of an existing secondary index used to succeed, and the name
    // conflicted with the index - the index definition was overwritten by the primary index of the new table, and
    // DML on the new table accessed the storage of the index
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t (c0 int primary key, c1 int)");
    execute_statement("CREATE INDEX i ON t (c1)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO t VALUES (1, 7)", stmt));
    test_stmt_err(
        "CREATE TABLE i (c0 int primary key)",
        error_code::target_already_exists_exception,
        "Index \"i\" already exists. Table name must be different from the existing index names."
    );
    // IF NOT EXISTS doesn't help because the table doesn't exist
    test_stmt_err("CREATE TABLE IF NOT EXISTS i (c0 int primary key)", error_code::target_already_exists_exception);
    {
        auto tx = utils::create_transaction(*db_);
        ASSERT_EQ(status::ok, tx->execute(*stmt));
        ASSERT_EQ(status::ok, tx->commit());
    }
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t WHERE c1 = 7", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4>(1, 7)), result[0]);
}

TEST_F(exclusive_ddl_dml_test, non_overwrapping_grant_and_dml) {
    // two dml operations before/after grant in the same dml tx whicle ddl tx instantly completes
    // TODO currently there is no error on this case, but DDL must have changed the metadata, so DML touching the updated table should fail
    utils::set_global_tx_option(utils::create_tx_option{false, true});  // use occ for simplicity
    execute_statement("CREATE TABLE t0 (c0 int primary key)");
    execute_statement("INSERT INTO t0 values (1)");
    auto tx0 = utils::create_transaction(*db_);
    {
        std::vector<mock::basic_record> result{};
        execute_query("select count(*) from t0", *tx0, result);
        EXPECT_EQ(1, result.size());
    }
    {
        auto tx1 = utils::create_transaction(*db_);
        execute_statement("grant select on table t0 to public", *tx1);
        execute_statement("revoke select on table t0 from public", *tx1);
        EXPECT_EQ(status::ok, tx1->commit());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("select count(*) from t0", *tx0, result);
        EXPECT_EQ(1, result.size());
    }
    EXPECT_EQ(status::ok, tx0->commit());
}



}
