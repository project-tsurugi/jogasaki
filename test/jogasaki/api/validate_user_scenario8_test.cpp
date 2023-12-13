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

#include <takatori/type/int.h>
#include <takatori/util/downcast.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/executor/common/graph.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/utils/storage_data.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;
using kind = meta::field_type_kind;

class validate_user_scenario8_test :
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

TEST_F(validate_user_scenario8_test, upsert_primary_abort) {
    // once aborting after upsert left the record resulting in scan failed
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot abort the changes";
    }
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("create table T (C0 int primary key, C1 int)");
    execute_statement("insert into T values (0,0)");
    wait_epochs(2);

    auto tx = utils::create_transaction(*db_, false, true, {"T"});
    execute_statement("insert or replace into T values (1,1)", *tx);
    wait_epochs(2);
    ASSERT_EQ(status::ok, tx->abort());
    wait_epochs(2);

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T", result);
    ASSERT_EQ(1, result.size());
}

TEST_F(validate_user_scenario8_test, upsert_secondaries_abort) {
    // once aborting after upsert (to secondary) left the record
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot abort the changes";
    }
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("create table T (C0 int primary key, C1 int)");
    execute_statement("create index I on T(C1)");
    execute_statement("insert into T values (0,0)");

    auto tx = utils::create_transaction(*db_, false, true, {"T"});
    execute_statement("insert into T values (1,1)", *tx);
    wait_epochs(2);
    ASSERT_EQ(status::ok, tx->abort());

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T WHERE C1=1", result);
    ASSERT_EQ(0, result.size());
}

}
