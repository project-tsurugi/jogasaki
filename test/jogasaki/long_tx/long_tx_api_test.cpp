/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/utils/tables.h>

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
        db_setup(cfg);

        auto* impl = db_impl();
        utils::add_test_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(long_tx_api_test, insert_to_non_preserved) {
    auto tx = utils::create_transaction(*db_, false, true, {});
    test_stmt_err("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx, error_code::ltx_write_operation_without_write_preserve_exception);
    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}

TEST_F(long_tx_api_test, update_to_non_preserved) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    auto tx = utils::create_transaction(*db_, false, true, {});
    test_stmt_err("UPDATE T0 SET C1=10.0 WHERE C0=1", *tx, error_code::ltx_write_operation_without_write_preserve_exception);
    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}

TEST_F(long_tx_api_test, delete_to_non_preserved) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)");
    auto tx = utils::create_transaction(*db_, false, true, {});
    test_stmt_err("DELETE FROM T0 WHERE C0=1", *tx, error_code::ltx_write_operation_without_write_preserve_exception);
    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}

TEST_F(long_tx_api_test, reading_outside_read_area) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("CREATE TABLE S (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("CREATE TABLE W (C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (1, 1)");
    {
        auto tx = utils::create_transaction(*db_, false, true, {"W"}, {}, {"T"}, "TEST");
        test_stmt_err("SELECT * FROM T WHERE C0=1", *tx, error_code::read_operation_on_restricted_read_area_exception);
        ASSERT_EQ(status::err_inactive_transaction, tx->commit());
    }
    {
        auto tx = utils::create_transaction(*db_, false, true, {"W"}, {"S"}, {}, "TEST");
        test_stmt_err("SELECT * FROM T WHERE C0=1", *tx, error_code::read_operation_on_restricted_read_area_exception);
        ASSERT_EQ(status::err_inactive_transaction, tx->commit());
    }
    {
        auto tx = utils::create_transaction(*db_, false, true, {"W"}, {"S"}, {"T"}, "TEST");
        test_stmt_err("SELECT * FROM T WHERE C0=1", *tx, error_code::read_operation_on_restricted_read_area_exception);
        ASSERT_EQ(status::err_inactive_transaction, tx->commit());
    }
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
    ASSERT_EQ(status::err_serialization_failure, tx2->commit());
}

TEST_F(long_tx_api_test, verify_key_locator_with_char) {
    // same as verify_key_locator tc, but using varlen string for key
    // erroneous key and storage name should be dumped in the server log
    execute_statement("CREATE TABLE T (C0 VARCHAR(100) PRIMARY KEY, C1 INT) ");
    execute_statement("INSERT INTO T (C0, C1) VALUES ('11111111111111111111111111111111', 1)");
    execute_statement("INSERT INTO T (C0, C1) VALUES ('22222222222222222222222222222222', 2)");
    auto tx1 = utils::create_transaction(*db_, false, true, {"T"});
    auto tx2 = utils::create_transaction(*db_, false, true, {"T"});
    execute_statement("UPDATE T SET C1=1 WHERE C0='11111111111111111111111111111111'", *tx1);
    execute_statement("UPDATE T SET C1=2 WHERE C0='11111111111111111111111111111111'", *tx2);
    ASSERT_EQ(status::ok, tx1->commit());
    ASSERT_EQ(status::err_serialization_failure, tx2->commit());
}

TEST_F(long_tx_api_test, begin_tx_with_bad_wp_name) {
    auto tx = utils::create_transaction(*db_, false, true, {"dummy_table"});
    ASSERT_FALSE(tx);
}

TEST_F(long_tx_api_test, begin_tx_with_bad_read_area_name) {
    {
        auto tx = utils::create_transaction(*db_, false, true, {}, {"dummy_table"});
        ASSERT_FALSE(tx);
    }
    {
        auto tx = utils::create_transaction(*db_, false, true, {}, {}, {"dummy_table"});
        ASSERT_FALSE(tx);
    }
}

TEST_F(long_tx_api_test, wps_added_to_rai) {
    // verify wps are in read area inclusive
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("CREATE TABLE R (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO R VALUES (10)");
    {
        // no read area inclusive meaning all tables readable
        auto tx = utils::create_transaction(*db_, false, true, {"T"}, {});
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx, result);
            ASSERT_EQ(1, result.size());
        }
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        // wp is added to read area inclusive
        auto tx = utils::create_transaction(*db_, false, true, {"T"}, {"R"});
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx, result);
            ASSERT_EQ(1, result.size());
        }
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        // duplicate entries safely ignored
        auto tx = utils::create_transaction(*db_, false, true, {"T"}, {"R", "T"});
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx, result);
            ASSERT_EQ(1, result.size());
        }
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        // exclusive wins if specified
        auto tx = utils::create_transaction(*db_, false, true, {"T"}, {"R"}, {"T"});
        test_stmt_err("SELECT * FROM T", *tx, error_code::read_operation_on_restricted_read_area_exception);
    }
}

TEST_F(long_tx_api_test, multiple_read_areas_variations) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("CREATE TABLE R (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO R VALUES (10)");
    {
        // inclusive read area only
        auto tx = utils::create_transaction(*db_, false, true, {}, {"R"}, {});
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM R", *tx, result);
            ASSERT_EQ(1, result.size());
        }
        test_stmt_err("SELECT * FROM T", *tx, error_code::read_operation_on_restricted_read_area_exception);
        ASSERT_EQ(status::err_inactive_transaction, tx->commit());
    }
    {
        // exclusive read area only
        auto tx = utils::create_transaction(*db_, false, true, {}, {}, {"R"});
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx, result);
            ASSERT_EQ(1, result.size());
        }
        test_stmt_err("SELECT * FROM R", *tx, error_code::read_operation_on_restricted_read_area_exception);
    }
    {
        // inclusive and exclusive specified
        auto tx = utils::create_transaction(*db_, false, true, {}, {"T"}, {"R"});
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx, result);
            ASSERT_EQ(1, result.size());
        }
        test_stmt_err("SELECT * FROM R", *tx, error_code::read_operation_on_restricted_read_area_exception);
        ASSERT_EQ(status::err_inactive_transaction, tx->commit());
    }
    {
        // duplicate inclusive and exclusive
        auto tx = utils::create_transaction(*db_, false, true, {}, {"T","T"}, {"R","R"});
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx, result);
            ASSERT_EQ(1, result.size());
        }
        test_stmt_err("SELECT * FROM R", *tx, error_code::read_operation_on_restricted_read_area_exception);
        ASSERT_EQ(status::err_inactive_transaction, tx->commit());
    }
    {
        // same table in inclusive and exclusive
        {
            auto tx = utils::create_transaction(*db_, false, true, {}, {"T", "R"}, {"R", "T"});
            test_stmt_err("SELECT * FROM T", *tx, error_code::read_operation_on_restricted_read_area_exception);
        }
        {
            auto tx = utils::create_transaction(*db_, false, true, {}, {"T", "R"}, {"R", "T"});
            test_stmt_err("SELECT * FROM R", *tx, error_code::read_operation_on_restricted_read_area_exception);
        }
    }
}

TEST_F(long_tx_api_test, ra_with_occ) {
    // verify simply ignored
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("CREATE TABLE R (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO R VALUES (10)");
    auto tx = utils::create_transaction(*db_, false, false, {}, {"T"});
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", *tx, result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM R", *tx, result);
        ASSERT_EQ(1, result.size());
    }
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(long_tx_api_test, ra_with_rtx) {
    // verify simply ignored
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("CREATE TABLE R (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO R VALUES (10)");
    auto tx = utils::create_transaction(*db_, true, false, {}, {"T"});
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", *tx, result);
        ASSERT_EQ(1, result.size());
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM R", *tx, result);
        ASSERT_EQ(1, result.size());
    }
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(long_tx_api_test, wp_with_occ) {
    // error if wp is specified for occ
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    execute_statement("CREATE TABLE R (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO R VALUES (10)");
    auto tx = utils::create_transaction(*db_, false, false, {"T"});
    ASSERT_FALSE(tx);
}

TEST_F(long_tx_api_test, wp_with_rtx) {
    // error if wp is specified for rtx
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    auto tx = utils::create_transaction(*db_, true, false, {"T"});
    ASSERT_FALSE(tx);
}

TEST_F(long_tx_api_test, error_info_occ_reading_wp) {
    utils::set_global_tx_option(utils::create_tx_option{false, true}); // to run with occ
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (1)");
    auto ltx = utils::create_transaction(*db_, false, true, {"T"});
    auto occ = utils::create_transaction(*db_);
    test_stmt_err("SELECT * FROM T", *occ, error_code::cc_exception);
}
}
