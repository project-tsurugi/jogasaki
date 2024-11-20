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
using jogasaki::mock::create_nullable_record;
using kind = meta::field_type_kind;

class select_reorder_test :
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

class block_verifier {
public:

    auto exec(std::function<void(void)> f, std::size_t wait_ns = 10*1000*1000) {
        using namespace std::chrono_literals;
        std::atomic_bool finished{false};
        auto g = std::async(std::launch::async, [=](){
            f();
            finished_ = true;
        });
        g.wait_for(std::chrono::nanoseconds{wait_ns});
        return g;
    }

    bool finished() {
        return finished_;
    }
private:
    std::atomic_bool finished_{false};
};

TEST_F(select_reorder_test, point_read_forwarded) {
    // simple scenario to verify forwarding by an anti-dependency
    // low priority tx1 (select) is forwarded before high priority tx0 (upsert)
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (2, 2)");
    {
        auto tx0 = utils::create_transaction(*db_, false, true, {"T"});
        wait_epochs(1);
        auto tx1 = utils::create_transaction(*db_, false, true, {"T"});
        execute_statement("INSERT OR REPLACE INTO T (C0, C1) VALUES (2, 20)", *tx0);
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T WHERE C0=2", *tx1, result);
            ASSERT_EQ(1, result.size());
            EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,2)), result[0]);
        }
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::ok, tx1->commit());
    }
}

TEST_F(select_reorder_test, range_read_forwarded) {
    // same as point_read_forwarded, except range read is used
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (2, 2)");
    {
        auto tx0 = utils::create_transaction(*db_, false, true, {"T"});
        wait_epochs(1);
        auto tx1 = utils::create_transaction(*db_, false, true, {"T"});
        execute_statement("INSERT OR REPLACE INTO T (C0, C1) VALUES (2, 20)", *tx0);
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx1, result);
            ASSERT_EQ(1, result.size());
            EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,2)), result[0]);
        }
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::ok, tx1->commit());
    }
}

TEST_F(select_reorder_test, forward_fail) {
    // typical scenario of forward failure by two read-modify-write transactions
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (2, 2)");
    {
        auto tx0 = utils::create_transaction(*db_, false, true, {"T"});
        wait_epochs(1);
        auto tx1 = utils::create_transaction(*db_, false, true, {"T"});
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T WHERE C0=2", *tx0, result);
            ASSERT_EQ(1, result.size());
            EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,2)), result[0]);
        }
        execute_statement("INSERT OR REPLACE INTO T (C0, C1) VALUES (2, 20)", *tx0);
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T WHERE C0=2", *tx1, result);
            ASSERT_EQ(1, result.size());
            EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,2)), result[0]);
        }
        execute_statement("INSERT OR REPLACE INTO T (C0, C1) VALUES (2, 30)", *tx1);
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::err_serialization_failure, tx1->commit());
    }
}

TEST_F(select_reorder_test, range_read_forward_fail) {
    // same as forward_fail, except range read is used
    execute_statement("CREATE TABLE T(C0 INT PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T (C0, C1) VALUES (2, 2)");
    {
        auto tx0 = utils::create_transaction(*db_, false, true, {"T"});
        wait_epochs(1);
        auto tx1 = utils::create_transaction(*db_, false, true, {"T"});
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx0, result);
            ASSERT_EQ(1, result.size());
            EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,2)), result[0]);
        }
        execute_statement("INSERT OR REPLACE INTO T (C0, C1) VALUES (2, 20)", *tx0);
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT * FROM T", *tx1, result);
            ASSERT_EQ(1, result.size());
            EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,2)), result[0]);
        }
        execute_statement("INSERT OR REPLACE INTO T (C0, C1) VALUES (2, 30)", *tx1);
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::err_serialization_failure, tx1->commit());
    }
}
}
