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

class validate_user_scenario_test :
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
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(validate_user_scenario_test, join_scan) {
    execute_statement("create table history ("
                      "caller_phone_number varchar(15) not null,"
                      "recipient_phone_number varchar(15) not null,"
                      "payment_categorty char(1) not null,"
                      "start_time bigint not null,"
                      "time_secs int not null,"
                      "charge int,"
                      "df int not null,"
                      "primary key (caller_phone_number, start_time)"
                      ")"
    );
    execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('001', '002', 'A', 20220505, 0, 0, 0)");
//    execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('001', '002', 'A', 20230101, 0, 0, 0)");
//    execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('010', '002', 'A', 20220505, 0, 0, 0)");

    execute_statement("create table contracts ("
                      "phone_number varchar(15) not null,"
                      "start_date bigint not null,"
                      "end_date bigint,"
                      "charge_rule varchar(255) not null,"
                      "primary key (phone_number, start_date)"
                      ")"
    );
    execute_statement("INSERT INTO contracts (phone_number,start_date,end_date,charge_rule)VALUES ('001', 20220101, 20221231, 'XXX')");
//    execute_statement("INSERT INTO contracts (phone_number,start_date,end_date,charge_rule)VALUES ('010', 20220101, 20221231, 'XXX')");
    std::vector<mock::basic_record> result{};
    execute_query("select "
                  "h.caller_phone_number, h.recipient_phone_number,  h.payment_categorty, h.start_time, h.time_secs, "
                  "h.charge, h.df "
                  "from history h inner join contracts c on c.phone_number = h.caller_phone_number "
//                  "from history h, contracts c where c.phone_number = h.caller_phone_number and "
                  "where c.start_date < h.start_time "
                  "and h.start_time < c.end_date + 1 "
                  "and c.phone_number = '001' "
                  "order by h.start_time"
                  ,
        result);
    ASSERT_EQ(1, result.size());
}

TEST_F(validate_user_scenario_test, join_scan_primary_key_specified) {
    execute_statement("create table history ("
                      "caller_phone_number varchar(15) not null,"
                      "recipient_phone_number varchar(15) not null,"
                      "payment_categorty char(1) not null,"
                      "start_time bigint not null,"
                      "time_secs int not null,"
                      "charge int,"
                      "df int not null,"
                      "primary key (caller_phone_number, start_time)"
                      ")"
    );
    execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('001', '002', 'A', 20220505, 0, 0, 0)");
//    execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('001', '002', 'A', 20230101, 0, 0, 0)");
//    execute_statement("INSERT INTO history (caller_phone_number,recipient_phone_number,payment_categorty,start_time,time_secs,charge,df)VALUES ('010', '002', 'A', 20220505, 0, 0, 0)");

    execute_statement("create table contracts ("
                      "phone_number varchar(15) not null,"
                      "start_date bigint not null,"
                      "end_date bigint,"
                      "charge_rule varchar(255) not null,"
                      "primary key (phone_number, start_date)"
                      ")"
    );
    execute_statement("INSERT INTO contracts (phone_number,start_date,end_date,charge_rule)VALUES ('001', 20220101, 20221231, 'XXX')");
//    execute_statement("INSERT INTO contracts (phone_number,start_date,end_date,charge_rule)VALUES ('010', 20220101, 20221231, 'XXX')");
    std::vector<mock::basic_record> result{};
    execute_query(
        "select"
        " h.caller_phone_number, h.recipient_phone_number, h.payment_categorty, h.start_time, h.time_secs,"
        " h.charge, h.df from history h"
        " inner join contracts c on c.phone_number = h.caller_phone_number"
        " where c.start_date < h.start_time and (h.start_time < c.end_date + 1"
        " or c.end_date = 99999999)"
        " and c.phone_number = '001' and c.start_date = 20220101 and h.caller_phone_number = '001' order by h.start_time",
        result);
    ASSERT_EQ(1, result.size());
}

TEST_F(validate_user_scenario_test, self_read_after_update) {
    // test scenario coming from batch verify
    execute_statement("create table test (foo int, bar bigint, zzz varchar(10), primary key(foo))");
    execute_statement("INSERT INTO test (foo, bar, zzz) VALUES (0,  0, '000')");

    auto tx = utils::create_transaction(*db_, false, false);
    execute_statement("INSERT INTO test (foo, bar, zzz) VALUES (123, 123, '123')", *tx);
    execute_statement("UPDATE test SET bar = 100 where foo = 123", *tx);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT foo, bar, zzz FROM test", *tx, result);
    EXPECT_EQ(2, result.size());
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(validate_user_scenario_test, select_date) {
    // test scenario coming from batch verify
    execute_statement("create table test (c0 int primary key, c1 date)");

    auto d2000_1_1 = date_v{2000, 1, 1};
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p1", api::field_type_kind::date},
    };
    auto ps = api::create_parameter_set();
    ps->set_date("p1", d2000_1_1);
    execute_statement("INSERT INTO test (c0, c1) VALUES (1, :p1)", variables, *ps);

    std::vector<mock::basic_record> result{};

    execute_query("SELECT * FROM test where c1 <= :p1", variables, *ps, result);
    ASSERT_EQ(1, result.size());

    using kind = meta::field_type_kind;
    auto i4 = meta::field_type{meta::field_enum_tag<kind::int4>};
    auto dat = meta::field_type{meta::field_enum_tag<kind::date>};
    EXPECT_EQ((mock::typed_nullable_record<kind::int4, kind::date>(std::tuple{i4, dat}, {1, d2000_1_1})), result[0]);
}

}
