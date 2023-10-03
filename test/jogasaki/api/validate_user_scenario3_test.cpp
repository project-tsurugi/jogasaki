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

#include <takatori/util/downcast.h>
#include <takatori/type/int.h>

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

class validate_user_scenario3_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_index_join(false); // to workaround problem caused by join_scan not implemented yet //TODO
        cfg->single_thread(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

// select distinct used to fail skipping records needed for limit=1
TEST_F(validate_user_scenario3_test, test_distinct_int) {
    execute_statement("create table T("
                      "c0 int not null"
                      ")"
    );
    execute_statement("INSERT INTO T (c0)VALUES (0)");
    execute_statement("INSERT INTO T (c0)VALUES (0)");
    execute_statement("INSERT INTO T (c0)VALUES (5)");
    {
        std::vector<mock::basic_record> result{};
        execute_query(
            "select distinct c0 from T",
            result);
        ASSERT_EQ(2, result.size());
    }
}
TEST_F(validate_user_scenario3_test, test_distinct) {
    execute_statement("create table history ("
                      "caller_phone_number varchar(15) not null"
                      ")"
    );
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000002')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000002')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000006')");
    {
        std::vector<mock::basic_record> result{};
        execute_query(
            "select distinct caller_phone_number from history",
            result);
        ASSERT_EQ(2, result.size());
    }
}

TEST_F(validate_user_scenario3_test, test_distinct_full) {
    execute_statement("create table history ("
                      "caller_phone_number varchar(15) not null"
                      ")"
    );
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000002')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000002')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000003')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000004')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000005')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000006')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000006')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000008')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000008')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000009')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000009')");

    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000010')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000011')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000011')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000011')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000012')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000013')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000013')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000016')");

    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000020')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000020')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000020')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000023')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000026')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000026')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000026')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000026')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000028')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000029')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000029')");

    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000032')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000032')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000032')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000032')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000032')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000034')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000036')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000037')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000037')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000039')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000040')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000040')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000040')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000040')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000042')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000042')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000042')");
    execute_statement("INSERT INTO history (caller_phone_number)VALUES ('00000000043')");
    {
        std::vector<mock::basic_record> result{};
        execute_query(
            "select distinct caller_phone_number from history",
            result);
        ASSERT_EQ(25, result.size());
    }
}
}
