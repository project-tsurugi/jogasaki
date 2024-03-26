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

#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class validate_qa_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->prepare_test_tables(true);
        db_setup(cfg);
        execute_statement("create table qa_t1 (c_pk int primary key, c_i4 int not null, c_i8 bigint not null, c_f4 real not null, c_f8 double not null, c_ch varchar(*) not null)");
        execute_statement("create index qa_t1_i4_idx on qa_t1(c_i4)");
        execute_statement("create table qa_t2 ( c_pk1 int not null, c_pk2 varchar(*) not null, c_id1 int not null, c_id2 varchar(*) null, c_jk1 int not null, c_jk2 varchar(*) null, primary key (c_pk1, c_pk2))");
        execute_statement("create index qa_t2_idc_idx on qa_t2(c_id1, c_id2)");
        execute_statement("create index qa_t2_jk1_idx on qa_t2(c_jk1)");
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(validate_qa_test, insert_after_delete_with_secondary_indices) {
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
    execute_statement("DELETE FROM qa_t1");
    wait_epochs();
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM qa_t1", result);
    ASSERT_EQ(1, result.size());
}

TEST_F(validate_qa_test, test_secondary_t2) {
    execute_statement( "INSERT INTO qa_t2 (c_pk1, c_pk2, c_id1, c_id2, c_jk1, c_jk2) VALUES (1, '10', 100, '1000', 10000, '100000')");
    execute_statement( "DELETE FROM qa_t2");
    wait_epochs();
    execute_statement( "INSERT INTO qa_t2 (c_pk1, c_pk2, c_id1, c_id2, c_jk1, c_jk2) VALUES (1, '10', 100, '1000', 10000, '100000')");
    execute_statement( "INSERT INTO qa_t2 (c_pk1, c_pk2, c_id1, c_id2, c_jk1, c_jk2) VALUES (2, '20', 200, '2000', 20000, '200000')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM qa_t2", result);
        ASSERT_EQ(2, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM qa_t2 WHERE c_id1=200 AND c_id2='2000'", result);
        ASSERT_EQ(1, result.size());
    }
}

TEST_F(validate_qa_test, long_tx_with_qa_table) {
    auto tx = utils::create_transaction(*db_, false, true, {"qa_t1"});
    execute_statement( "INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')", *tx);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c_pk FROM qa_t1 where c_pk=1", *tx, result);
    ASSERT_EQ(1, result.size());
    execute_statement( "INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (2, 20, 200, 2000.0, 20000.0, '200000')", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(validate_qa_test, verify_invalid_state) {
    execute_statement( "INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
    auto tx = utils::create_transaction(*db_, false, true, {"qa_t1"});
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c_pk FROM qa_t1 where c_pk=1", *tx, result);
    ASSERT_EQ(1, result.size());
    execute_statement( "INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (2, 20, 200, 2000.0, 20000.0, '200000')", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(validate_qa_test, verify_invalid_state_on_non_qa_table) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    auto tx = utils::create_transaction(*db_, false, true, {"T0"});
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 FROM T0 where C0=1", *tx, result);
    ASSERT_EQ(1, result.size());
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}


TEST_F(validate_qa_test, simplified_crash_on_wp_build) {
    // using T0 instead of qa tables
    {
        execute_statement("delete from T0 where C0=1");
        execute_statement("INSERT INTO T0(C0, C1) VALUES (1, 10.0)");
        std::vector<mock::basic_record> result{};
        execute_query("select C1 from T0 where C0=1", result);
        EXPECT_EQ(1, result.size());
    }
    {
        execute_statement("delete from T0 where C0=1");
        wait_epochs(2); // delete have to wait. timing issue seen on Ubuntu20
        execute_statement("INSERT INTO T0(C0, C1) VALUES (1, 10.0)");
        std::vector<mock::basic_record> result{};
        execute_query("select C1 from T0 where C0=1", result);
        EXPECT_EQ(1, result.size());
    }
}

TEST_F(validate_qa_test, long_update) {
    // insertint to same page. This scenario once blocked and waited forever.
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
    auto tx = utils::create_transaction(*db_, false, true, {"qa_t1"});
    execute_statement("update qa_t1 set c_i4 = 3 where c_pk = 1", *tx);
    ASSERT_EQ(status::ok, tx->commit());
    std::vector<mock::basic_record> result{};
    execute_query("select c_i4 from qa_t1 where c_pk=1", result);
    EXPECT_EQ(1, result.size());
}

TEST_F(validate_qa_test, verify_non_null_error_with_update) {
    // once UPDATE failure lost records being updated
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback on error";
    }
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int not null primary key, C1 int)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (1,1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (2,2)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (3,3)");
    execute_statement("UPDATE T SET C0=null WHERE C0=1", status::err_integrity_constraint_violation, true);  // set no_abort=true to verify tx aborted automatically
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from T", result);
        ASSERT_EQ(3, result.size());
    }
}

TEST_F(validate_qa_test, verify_non_null_error_with_update_no_pred) {
    // once UPDATE failure lost records being updated
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback on error";
    }
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int not null primary key, C1 int)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (1,1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (2,2)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (3,3)");
    execute_statement("UPDATE T SET C0=null", status::err_integrity_constraint_violation, true);  // set no_abort=true to verify tx aborted automatically
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from T", result);
        ASSERT_EQ(3, result.size());
    }
}

TEST_F(validate_qa_test, delete_with_secondary) {
    // once UPDATE->DELETE caused NOT_FOUND error
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot rollback on error";
    }
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int not null primary key, C1 int, C2 char(2))");
    execute_statement("create index I on T(C2)");
    execute_statement("INSERT INTO T (C0, C1, C2) VALUES (1,1, '1')");
    execute_statement("UPDATE T SET C1=2, C2='2' WHERE C0=1");
    execute_statement("DELETE FROM T");
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from T", result);
        ASSERT_EQ(0, result.size());
    }
}

TEST_F(validate_qa_test, delete_insert_met_already_exists) {
    // once DELETE->INSERT caused ALREADY EXISTS error (issue #26)
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "this test is regression scenario only for jogasaki-shirakami";
    }
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    {
        auto tx = utils::create_transaction(*db_, false, false, {});
        execute_statement("delete from qa_t1", *tx);
        execute_statement("insert into qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) values (1, 0, 0, 0, 0, 'a')", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }

    auto tx1 = utils::create_transaction(*db_, false, false, {});
    execute_statement("update qa_t1 set c_i4 = 1 where c_pk = 1", *tx1);
    auto tx2 = utils::create_transaction(*db_, false, false, {});
    execute_statement("delete from qa_t1", *tx2);
    execute_statement("insert into qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) values (1, 1, 0, 0, 0, 'x')", *tx2); // once this caused ALREADY EXISTS error
    ASSERT_EQ(status::ok, tx2->commit());
    ASSERT_EQ(status::err_serialization_failure, tx1->commit());  //KVS_DELETE
    {
        std::vector<mock::basic_record> result{};
        execute_query("select * from qa_t1", result);
        ASSERT_EQ(1, result.size());
    }
}

}
