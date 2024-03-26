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

#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

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

class validate_user_scenario2_test :
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

TEST_F(validate_user_scenario2_test, phone_bill_history_table) {
    // test scenario coming from batch verify
    execute_statement("create table history ("
                      "caller_phone_number varchar(15) not null,"
                      "recipient_phone_number varchar(15) not null,"
                      "payment_categorty char(1) not null,"
                      "start_time timestamp not null,"
                      "time_secs int not null,"
                      "charge int,"
                      "df int not null,"
                      "primary key (caller_phone_number, payment_categorty, start_time)"
                      ")");
    execute_statement("create index history_df_idx on history(df)");
    execute_statement("create index idx_st on history(start_time)");
    execute_statement("create index idx_rp on history(recipient_phone_number, payment_categorty, start_time)");

    auto d2000_1_1 = date_v{2000, 1, 1};
    auto d2000_5_5 = date_v{2000, 5, 5};
    auto d2000_6_6 = date_v{2000, 6, 6};
    auto d2000_12_31 = date_v{2000, 12, 31};
    auto t12_0_0 = time_of_day_v{12, 0, 0};
    auto tp2000_1_1_12_0_0 = time_point_v{d2000_1_1, t12_0_0};
    auto tp2000_5_5_12_0_0 = time_point_v{d2000_5_5, t12_0_0};
    auto tp2000_6_6_12_0_0 = time_point_v{d2000_6_6, t12_0_0};
    auto tp2000_12_31_12_0_0 = time_point_v{d2000_12_31, t12_0_0};

    {
        // prepare data
        auto ps = api::create_parameter_set();
        ps->set_time_point("start_time", tp2000_5_5_12_0_0);

        std::unordered_map<std::string, api::field_type_kind> variables{
            {"start_time", api::field_type_kind::time_point},
        };
        execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('001', '002', 'C', :start_time, 10, 100, 0)", variables, *ps);
        execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('003', '001', 'R', :start_time, 20, 200, 0)", variables, *ps);
        execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('004', '005', 'R', :start_time, 20, 200, 0)", variables, *ps);
        execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('006', '001', 'C', :start_time, 20, 200, 0)", variables, *ps);

        ps->set_time_point("start_time", tp2000_6_6_12_0_0);
        execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('001', '002', 'R', :start_time, 20, 200, 0)", variables, *ps);
    }

    {
        auto ps = api::create_parameter_set();
        ps->set_time_point("start", tp2000_1_1_12_0_0);
        ps->set_time_point("end", tp2000_12_31_12_0_0);
        ps->set_character("caller_phone_number", "001");
        ps->set_character("recipient_phone_number", "001");

        std::unordered_map<std::string, api::field_type_kind> variables{
            {"start", api::field_type_kind::time_point},
            {"end", api::field_type_kind::time_point},
            {"caller_phone_number", api::field_type_kind::character},
            {"recipient_phone_number", api::field_type_kind::character},
        };

        {
            // test original query
            std::vector<mock::basic_record> result{};
            execute_query(
                "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs,"
                " charge, df from history "
                "where start_time >= :start and start_time < :end"
                " and ((caller_phone_number = :caller_phone_number  and payment_categorty = 'C') "
                "  or (recipient_phone_number = :recipient_phone_number and payment_categorty = 'R'))"
                " and df = 0",
                variables, *ps, result);

            ASSERT_EQ(2, result.size());
            using kind = meta::field_type_kind;
        }
        {
            // test separated first
            std::vector<mock::basic_record> result{};
            execute_query(
                "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs,"
                " charge, df from history "
                "where start_time >= :start and start_time < :end"
                " and caller_phone_number = :caller_phone_number  and payment_categorty = 'C' "
                " and df = 0",
                variables, *ps, result);

            ASSERT_EQ(1, result.size());
        }
        {
            // test separated second
            std::vector<mock::basic_record> result{};
            execute_query(
                "select caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs,"
                " charge, df from history "
                "where start_time >= :start and start_time < :end"
                " and recipient_phone_number = :recipient_phone_number and payment_categorty = 'R'"
                " and df = 0",
                variables, *ps, result);

            ASSERT_EQ(1, result.size());
        }
    }
}

}
