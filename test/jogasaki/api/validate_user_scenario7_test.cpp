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
using kind = meta::field_type_kind;

class validate_user_scenario7_test :
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

TEST_F(validate_user_scenario7_test, fixing_update_degrades_perf) {
    // scenario to manually debug perf issue. Automated test simply checks functionality.
    execute_statement(
        "create table history ("
        "caller_phone_number varchar(15) not null,"
        "recipient_phone_number varchar(15) not null,"
        "payment_category char(1) not null,"
        "start_time timestamp not null,"
        "time_secs int not null,"
        "charge int,"
        "df int not null,"
        "primary key (caller_phone_number, payment_category, start_time)"
        ")"
    );
    execute_statement("create index idx_st on history(start_time)");
    execute_statement("create index idx_rp on history(recipient_phone_number, payment_category, start_time)");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"start_time", api::field_type_kind::time_point},
    };
    auto d2000_1_1 = date_v{2000, 1, 1};
    auto t12_0_0 = time_of_day_v{12, 0, 0};
    auto tp2000_1_1_12_0_0 = time_point_v{d2000_1_1, t12_0_0};
    auto ps = api::create_parameter_set();
    ps->set_time_point("start_time", tp2000_1_1_12_0_0);

    execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_category,start_time,time_secs,charge,df)VALUES ('A', 'B', 'C', :start_time, 0, 0, 0)", variables, *ps);
    execute_statement("update history set recipient_phone_number = 'X', time_secs = 1, charge = 1, df = 1 where caller_phone_number = 'A' and payment_category = 'C' and start_time = :start_time",
        variables,
        *ps
    );
    std::vector<mock::basic_record> result{};
    execute_query("SELECT recipient_phone_number, time_secs, charge, df FROM history", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::character, kind::int4, kind::int4, kind::int4>(accessor::text{"X"}, 1,1,1)), result[0]);
}

}
