/*
 * Copyright 2018-2020 tsurugi project.
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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/type/int.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
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
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(validate_qa_test, insert_after_delete_with_secondary_indices) {
    execute_statement( "INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
    execute_statement( "DELETE FROM qa_t1");
    wait_epochs();
    execute_statement( "INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");

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
//    wait_epochs(10);
    auto tx = utils::create_transaction(*db_, false, true, {"qa_t1"});
    std::vector<mock::basic_record> result{};
    execute_query("SELECT c_pk FROM qa_t1 where c_pk=1", *tx, result);
    ASSERT_EQ(1, result.size());
//    execute_statement( "INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (2, 20, 200, 2000.0, 20000.0, '200000')", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(validate_qa_test, verify_invalid_state_on_non_qa_table) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    auto tx = utils::create_transaction(*db_, false, true, {"T0"});
    wait_epochs(10);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 FROM T0 where C0=1", *tx, result);
    ASSERT_EQ(1, result.size());
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (2, 20.0)", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(validate_qa_test, crash_on_wp_build) {
    // once this scenario crashed with BUILD_WP=ON
    {
//        execute_statement("delete from qa_t1 where c_pk=1");
        execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
        std::vector<mock::basic_record> result{};
        execute_query("select c_pk from qa_t1 where c_pk=1", result);
        EXPECT_EQ(1, result.size());
    }
    {
        execute_statement("delete from qa_t1 where c_pk=1");
//        wait_epochs(40);
        execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')");
        std::vector<mock::basic_record> result{};
        execute_query("select c_pk from qa_t1 where c_pk=1", result);
        EXPECT_EQ(1, result.size());
    }
}

TEST_F(validate_qa_test, simplified_crash_on_wp_build) {
    // using T0 instead of qa tables
    {
//        execute_statement("delete from qa_t1 where c_pk=1");
        execute_statement("INSERT INTO T0(C0, C1) VALUES (1, 10.0)");
        std::vector<mock::basic_record> result{};
        execute_query("select C1 from T0 where C0=1", result);
        EXPECT_EQ(1, result.size());
    }
    {
        execute_statement("delete from T0 where C0=1");
        wait_epochs(2);
        execute_statement("INSERT INTO T0(C0, C1) VALUES (1, 10.0)");
        std::vector<mock::basic_record> result{};
        execute_query("select C1 from T0 where C0=1", result);
        EXPECT_EQ(1, result.size());
    }
}

TEST_F(validate_qa_test, long_vs_short_insert1) {
    // inserting to same page. This scenario once blocked and waited forever.
    auto tx1 = utils::create_transaction(*db_, false, true, {"qa_t1"});
    auto tx2 = utils::create_transaction(*db_);
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')", *tx2, status::err_conflict_on_write_preserve);
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')", *tx1);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
}

TEST_F(validate_qa_test, long_vs_short_insert2) {
    // inserting to same page. This scenario once blocked and waited forever.
    auto tx1 = utils::create_transaction(*db_, false, true, {"qa_t1"});
    auto tx2 = utils::create_transaction(*db_);
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')", *tx1);
    execute_statement("INSERT INTO qa_t1 (c_pk, c_i4, c_i8, c_f4, c_f8, c_ch) VALUES (1, 10, 100, 1000.0, 10000.0, '100000')", *tx2, status::err_conflict_on_write_preserve);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::ok, tx2->commit());
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

}
