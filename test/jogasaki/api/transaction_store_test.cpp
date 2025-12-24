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

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class transaction_store_test :
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

TEST_F(transaction_store_test, basic) {
    // verify creating transaction creates store and deletes it on destroy
    execute_statement("create table t (c0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (0)");

    utils::set_global_tx_option(utils::create_tx_option{false, false, 999});
    {
        auto tx = utils::create_transaction(*db_impl());
        ASSERT_EQ(999, tx->session_id());
        execute_statement("INSERT INTO t VALUES (1)", *tx);

        auto t = db_impl()->find_transaction(*tx);
        EXPECT_TRUE(t);
        EXPECT_EQ(1, db_impl()->transaction_count());

        auto store = db_impl()->find_transaction_store(999);
        EXPECT_TRUE(store);
        EXPECT_EQ(1, store->size());
    }
    EXPECT_EQ(0, db_impl()->transaction_count());
    ASSERT_TRUE(db_impl()->find_transaction_store(999)); // store remains until transaction_store::dispose is called
}

TEST_F(transaction_store_test, multiple_transactions) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "memory bridge goes into infinite loop";
    }
    // verify creating transactions for one session
    execute_statement("create table t (c0 INT PRIMARY KEY)");
    execute_statement("INSERT INTO t VALUES (0)");

    utils::set_global_tx_option(utils::create_tx_option{false, false, 999});
    {
        auto tx0 = utils::create_transaction(*db_impl());
        ASSERT_EQ(999, tx0->session_id());
        auto t = db_impl()->find_transaction(*tx0);
        EXPECT_TRUE(t);
        EXPECT_EQ(1, db_impl()->transaction_count());

        auto store = db_impl()->find_transaction_store(999);
        EXPECT_TRUE(store);
        EXPECT_EQ(1, store->size());

        {
            auto tx1 = utils::create_transaction(*db_impl());
            ASSERT_EQ(999, tx1->session_id());
            auto t = db_impl()->find_transaction(*tx1);
            EXPECT_TRUE(t);
            EXPECT_EQ(2, db_impl()->transaction_count());

            auto store = db_impl()->find_transaction_store(999);
            EXPECT_TRUE(store);
            EXPECT_EQ(2, store->size());
        }
    }
    EXPECT_EQ(0, db_impl()->transaction_count());
    ASSERT_TRUE(db_impl()->find_transaction_store(999)); // store remains until transaction_store::dispose is called
}

TEST_F(transaction_store_test, crud_with_store) {
    // verify handle is not usable on different session
    utils::set_global_tx_option(utils::create_tx_option{false, false, 999});
    {
        auto tx = utils::create_transaction(*db_impl());
        ASSERT_EQ(999, tx->session_id());

        auto store = db_impl()->find_transaction_store(999);
        EXPECT_TRUE(store);
        EXPECT_EQ(1, store->size());
        EXPECT_EQ(999, store->session_id());
        auto tctx = store->lookup(*tx);
        EXPECT_TRUE(tctx);

        auto key = api::transaction_handle{1000, 999};
        EXPECT_TRUE(! store->lookup(key));
        EXPECT_TRUE(! store->remove(key));
        EXPECT_TRUE(! store->lookup(key));
        EXPECT_TRUE(store->put(key, std::move(tctx)));
        EXPECT_TRUE(store->lookup(key));
        EXPECT_EQ(2, store->size());
        EXPECT_TRUE(store->remove(key));
        EXPECT_EQ(1, store->size());
        store->dispose();
        EXPECT_EQ(0, db_impl()->transaction_count());
        EXPECT_TRUE(! db_impl()->find_transaction_store(999));
    }
}

}
