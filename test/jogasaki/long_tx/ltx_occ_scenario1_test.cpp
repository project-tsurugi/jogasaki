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
#include "../api/runner.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

// regression testcase issue #390 - starting ltx and then occ commit stopped sending new durability marker for the occ
class ltx_occ_scenario1_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->default_commit_response(commit_response_kind::stored);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

};

using namespace std::string_view_literals;

TEST_F(ltx_occ_scenario1_test, never_durable) {
    utils::set_global_tx_option({false, true});
    execute_statement("create table T1(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("create table T2(C0 INT NOT NULL PRIMARY KEY)");
    auto ltx = utils::create_transaction(*db_, false, true, {"T1"});
    auto occ = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, occ->commit());
    ASSERT_EQ(status::ok, ltx->commit());
}

TEST_F(ltx_occ_scenario1_test, never_durable_commit_from_other_thread) {
    utils::set_global_tx_option({false, true});
    execute_statement("create table T1(C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("create table T2(C0 INT NOT NULL PRIMARY KEY)");
    auto ltx = utils::create_transaction(*db_, false, true, {"T1"});
    execute_statement("INSERT INTO T1 VALUES (0)", *ltx);
    {
        auto f1 = std::async(std::launch::async, [&]() {
            auto occ = utils::create_transaction(*db_);
            execute_statement("INSERT INTO T2 VALUES (100)", *occ);
            ASSERT_EQ(status::ok, occ->commit());
        });
        f1.get();
    }
    execute_statement("INSERT INTO T1 VALUES (10)", *ltx);
    ASSERT_EQ(status::ok, ltx->commit());
}
}
