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
#include <jogasaki/kvs/id.h>
#include "../api/api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class transaction_and_ddl_test :
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

TEST_F(transaction_and_ddl_test, create_with_ltx_modifies_definitions) {
    api::transaction_option opts{};
    opts.is_long(true).modifies_definitions(true);
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("CREATE TABLE TT (C1 INT)", *tx);
        ASSERT_EQ(status::ok, tx->commit());
    }
    {
        // verify tx with modifies_definitions = true has access to system tables
        api::transaction_option opts{false, true, {"TT"}, "label", {"TT"}};
        opts.is_long(true).modifies_definitions(true);
        auto tx = utils::create_transaction(*db_, opts);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM "+std::string{system_sequences_name}, *tx, result);
        ASSERT_EQ(status::ok, tx->commit());
        ASSERT_LT(0, result.size());
    }
    execute_statement("INSERT INTO TT VALUES (1)");
    execute_statement("INSERT INTO TT VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT", result);
        ASSERT_EQ(2, result.size());
    }
}

TEST_F(transaction_and_ddl_test, create_with_ltx_wo_modifies_definitions) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory won't raise error with ddl on ltx";
    }
    api::transaction_option opts{};
    opts.is_long(true).modifies_definitions(false);
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("CREATE TABLE TT (C1 INT)", *tx, status::err_write_without_write_preserve);
    }
}

TEST_F(transaction_and_ddl_test, create_with_rtx) {
    api::transaction_option opts{};
    opts.readonly(true).modifies_definitions(false);
    {
        auto tx = utils::create_transaction(*db_, opts);
        execute_statement("CREATE TABLE TT (C1 INT)", *tx, status::err_illegal_operation);
    }
}
}
