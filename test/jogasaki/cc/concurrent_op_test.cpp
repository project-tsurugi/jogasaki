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

#include <future>
#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/utils/storage_data.h>

#include "../api/api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using jogasaki::api::impl::get_impl;
using takatori::util::unsafe_downcast;
using jogasaki::mock::create_nullable_record;
using kind = meta::field_type_kind;

class concurrent_op_test :
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
    void test_scan_err(
        api::transaction_handle tx,
        std::string_view index_name,
        status expected,
        std::size_t error_record_index
    );
};

using namespace std::string_view_literals;

void concurrent_op_test::test_scan_err(
    api::transaction_handle tx,
    std::string_view index_name,
    status expected,
    std::size_t error_record_index
) {
    auto index = get_impl(*db_).kvs_db()->get_storage(index_name);

    std::unique_ptr<kvs::iterator> it{};
    std::string buf{};
    auto tctx = api::get_transaction_context(tx);
    if(status::ok !=
       index->content_scan(*tctx->object(), buf, kvs::end_point_kind::unbound, buf, kvs::end_point_kind::unbound, it)) {
        fail();
    };
    std::size_t cnt = 0;
    while(status::ok == it->next()) {
        std::string_view key{};
        std::string_view value{};
        if(cnt == error_record_index) {
            if(expected != it->read_key(key)) {
                std::cerr << "cnt:" << cnt << std::endl;
                FAIL();
            };
        } else {
            if(status::ok != it->read_key(key)) {
                std::cerr << "cnt:" << cnt << std::endl;
                FAIL();
            };
        }
        if(cnt == error_record_index) {
            if(expected != it->read_value(value)) {
                std::cerr << "cnt:" << cnt << std::endl;
                FAIL();
            };
        } else {
            if(status::ok != it->read_value(value)) {
                std::cerr << "cnt:" << cnt << std::endl;
                FAIL();
            };
        }
        ++cnt;
    }
    it.reset();
}

TEST_F(concurrent_op_test, occ_scan_see_concurrent_insert) {
    // scan can skip concurrently inserted uncommitted records as if they don't exist
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (0)");
    execute_statement("INSERT INTO T VALUES (2)");
    {
        auto tx0 = utils::create_transaction(*db_, false, false);
        execute_statement("INSERT INTO T VALUES (1)", *tx0);

        auto tx1 = utils::create_transaction(*db_, false, false);
        test_scan_err(*tx1, "T", status::concurrent_operation, 1);

        ASSERT_EQ(status::ok, tx1->commit());
        ASSERT_EQ(status::ok, tx0->commit());
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", result);
            EXPECT_EQ(3, result.size());
        }
    }
}

TEST_F(concurrent_op_test, occ_scan_see_concurrent_insert_commit_fail) {
    // scan can skip concurrently inserted uncommitted records, but scan must commit before insert tx
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (0)");
    execute_statement("INSERT INTO T VALUES (2)");
    {
        auto tx0 = utils::create_transaction(*db_, false, false);
        execute_statement("INSERT INTO T VALUES (1)", *tx0);

        auto tx1 = utils::create_transaction(*db_, false, false);
        test_scan_err(*tx1, "T", status::concurrent_operation, 1); //TODO

        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::err_serialization_failure, tx1->commit()); //TODO
    }
}

TEST_F(concurrent_op_test, occ_scan_op_skips_concurrent_insert) {
    // scan op uses kvs scan and skips concurrently inserted uncommitted records as if they don't exist
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO T VALUES (0)");
    execute_statement("INSERT INTO T VALUES (2)");
    {
        auto tx0 = utils::create_transaction(*db_, false, false);
        execute_statement("INSERT INTO T VALUES (1)", *tx0);

        auto tx1 = utils::create_transaction(*db_, false, false);
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx1, result);
            EXPECT_EQ(2, result.size());
        }

        ASSERT_EQ(status::ok, tx1->commit());
        ASSERT_EQ(status::ok, tx0->commit());
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", result);
            EXPECT_EQ(3, result.size());
        }
    }
}

TEST_F(concurrent_op_test, occ_get_see_concurrent_insert) {
    // occ get aborts if it sees concurrently inserted uncommitted records
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY)");
    {
        auto tx0 = utils::create_transaction(*db_, false, false);
        execute_statement("INSERT INTO T VALUES (1)", *tx0);

        auto tx1 = utils::create_transaction(*db_, false, false);

        test_stmt_err("SELECT * FROM T WHERE C0=1", *tx1, error_code::cc_exception, "serialization failed transaction:TID-0000000100000002 shirakami response Status=OK {reason_code:USER_ABORT, storage_name is not available, no key information} ");

        ASSERT_EQ(status::ok, tx0->commit());
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", result);
            EXPECT_EQ(1, result.size());
        }
    }
}

TEST_F(concurrent_op_test, find_op_skips_concurrent_insert_on_secondary) {
    // occ find op uses kvs scan, observes concurrently inserted record on secodary, and skips it
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("CREATE INDEX I ON T(C1)");
    {
        auto tx0 = utils::create_transaction(*db_, false, false);
        execute_statement("INSERT INTO T VALUES (1, 10)", *tx0);
        auto tx1 = utils::create_transaction(*db_, false, false);
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx1, result);
            EXPECT_EQ(0, result.size());
        }

        ASSERT_EQ(status::ok, tx1->commit());
        ASSERT_EQ(status::ok, tx0->commit());
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", result);
            EXPECT_EQ(1, result.size());
        }
    }
}

TEST_F(concurrent_op_test, occ_insert_not_see_concurrent_insert) {
    // occ insert doesn't see concurrently inserted uncommitted records
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY)");
    {
        auto tx0 = utils::create_transaction(*db_, false, false);
        execute_statement("INSERT INTO T VALUES (1)", *tx0);
        auto tx1 = utils::create_transaction(*db_, false, false);
        execute_statement("INSERT INTO T VALUES (1)", *tx1);

        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::err_serialization_failure, tx1->commit()); // error by KVS_INSERT
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", result);
            EXPECT_EQ(1, result.size());
        }
    }
}

TEST_F(concurrent_op_test, occ_insert_not_see_concurrent_insert_reversed_commit_order) {
    // occ insert doesn't see concurrently inserted uncommitted records, and second commit fails
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY)");
    {
        auto tx0 = utils::create_transaction(*db_, false, false);
        execute_statement("INSERT INTO T VALUES (1)", *tx0);
        auto tx1 = utils::create_transaction(*db_, false, false);
        execute_statement("INSERT INTO T VALUES (1)", *tx1);

        ASSERT_EQ(status::ok, tx1->commit());
        ASSERT_EQ(status::err_serialization_failure, tx0->commit()); // error by KVS_INSERT
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", result);
            EXPECT_EQ(1, result.size());
        }
    }
}

}  // namespace jogasaki::testing
