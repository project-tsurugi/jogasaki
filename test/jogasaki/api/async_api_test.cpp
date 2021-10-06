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
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"
#include "../test_utils/temporary_folder.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class async_api_test :
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

TEST_F(async_api_test, async_insert) {
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", stmt));
    auto tx = db_->create_transaction();
    status s{};
    std::string message{"message"};
    std::atomic_bool run{false};
    tx->execute_async(*stmt, [&](status st, std::string_view msg){
        s = st;
        message = msg;
        run.store(true);
    });
    while(! run.load()) {}
    ASSERT_EQ(status::ok, s);
    ASSERT_TRUE(message.empty());
}

TEST_F(async_api_test, async_update) {
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    std::unique_ptr<api::executable_statement> stmt{};
    ASSERT_EQ(status::ok, db_->create_executable("UPDATE T0 SET C1=20.0 WHERE C0=1", stmt));
    auto tx = db_->create_transaction();
    status s{};
    std::string message{"message"};
    std::atomic_bool run{false};
    tx->execute_async(*stmt, [&](status st, std::string_view msg){
        s = st;
        message = msg;
        run.store(true);
    });
    while(! run.load()) {}
    ASSERT_EQ(status::ok, s);
    ASSERT_TRUE(message.empty());
}

}
