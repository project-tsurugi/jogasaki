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

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "../api/api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class long_tx_api_test :
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
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(long_tx_api_test, insert_to_non_preserved) {
    auto tx = utils::create_transaction(*db_, false, true, {});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx, status::err_illegal_operation);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(long_tx_api_test, update_to_non_preserved) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    auto tx = utils::create_transaction(*db_, false, true, {});
    execute_statement("UPDATE T0 SET C1=10.0 WHERE C0=1", *tx, status::err_illegal_operation);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(long_tx_api_test, delete_to_non_preserved) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    auto tx = utils::create_transaction(*db_, false, true, {});
    execute_statement("DELETE FROM T0 WHERE C0=1", *tx, status::err_illegal_operation);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(long_tx_api_test, verify_key_locator) {
    // borrowed multiple_tx_iud_same_key scenario in long_tx_test to verify commit error code handling
    // erroneous key and storage name should be dumped in the server log
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("UPDATE T0 SET C1=10.0 WHERE C0=1", *tx1);
    execute_statement("UPDATE T0 SET C1=20.0 WHERE C0=1", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::err_aborted_retryable, tx2->commit());
}

TEST_F(long_tx_api_test, verify_key_locator_with_char) {
    // same as verify_key_locator tc, but using varlen string for key
    // erroneous key and storage name should be dumped in the server log
    execute_statement("CREATE TABLE T (C0 VARCHAR(100) PRIMARY KEY, C1 INT) ");
    execute_statement("INSERT INTO T (C0, C1) VALUES ('11111111111111111111111111111111', 1.0)");
    execute_statement("INSERT INTO T (C0, C1) VALUES ('22222222222222222222222222222222', 2.0)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T"});
    execute_statement("UPDATE T SET C1=1 WHERE C0='11111111111111111111111111111111'", *tx1);
    execute_statement("UPDATE T SET C1=2 WHERE C0='11111111111111111111111111111111'", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::err_aborted_retryable, tx2->commit());
}
}
