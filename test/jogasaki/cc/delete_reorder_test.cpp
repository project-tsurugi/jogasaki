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
#include <future>

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

class delete_reorder_test :
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
        add_benchmark_tables(*impl->tables());
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

TEST_F(delete_reorder_test, insert_delete) {
    // low priority tx1 is forwarded before high priority tx0
    // high priority tx insert sees existing record (read operation), and low priority delete conflicts with it and its commit aborts
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    {
        // tx1 key based access
        auto tx0 = utils::create_transaction(*db_, false, true, {"T0"});
        auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx0, status::err_already_exists);
        execute_statement("SELECT * FROM T0 WHERE C0=1", *tx1);
        execute_statement("DELETE FROM T0 WHERE C0=1", *tx1);
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::err_aborted_retryable, tx1->commit());
    }
    {
        // tx1 scan based access
        auto tx0 = utils::create_transaction(*db_, false, true, {"T0"});
        auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx0, status::err_already_exists);
        execute_statement("SELECT * FROM T0", *tx1);
        execute_statement("DELETE FROM T0", *tx1);
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::err_aborted_retryable, tx1->commit());
    }
}

TEST_F(delete_reorder_test, delete_insert) {
    // low priority tx1 is forwarded before high priority tx0
    // high priority tx insert sees existing record (read operation), and low priority delete conflicts with it and its commit aborts
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)");
    {
        // tx1 key based access
        auto tx0 = utils::create_transaction(*db_, false, true, {"T0"});
        auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("SELECT * FROM T0 WHERE C0=1", *tx1);
        execute_statement("DELETE FROM T0 WHERE C0=1", *tx1);
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx0, status::err_already_exists); //TOOD correct?
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::err_aborted_retryable, tx1->commit());
    }
    {
        // tx1 scan based access
        auto tx0 = utils::create_transaction(*db_, false, true, {"T0"});
        auto tx1 = utils::create_transaction(*db_, false, true, {"T0"});
        execute_statement("SELECT * FROM T0", *tx1);
        execute_statement("DELETE FROM T0", *tx1);
        execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx0, status::err_already_exists); //TOOD correct?
        ASSERT_EQ(status::ok, tx0->commit());
        ASSERT_EQ(status::err_aborted_retryable, tx1->commit());
    }
}
}