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

class validate_batch_test :
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

TEST_F(validate_batch_test, join_scan) {
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
//    execute_statement("INSERT INTO contracts (phone_number,start_date,end_date,charge_rule)VALUES ('001', 20220101, 20221231, 'XXX')");
    execute_statement("INSERT INTO contracts (phone_number,start_date,end_date,charge_rule)VALUES ('010', 20220101, 20221231, 'XXX')");
    std::vector<mock::basic_record> result{};
    execute_query("select "
                  "h.caller_phone_number, h.recipient_phone_number,  h.payment_categorty, h.start_time, h.time_secs, "
                  "h.charge, h.df "
                  "from history h inner join contracts c on c.phone_number = h.caller_phone_number "
//                  "from history h, contracts c where c.phone_number = h.caller_phone_number and "
                  "where c.start_date < h.start_time "
                  "and h.start_time < c.end_date + 1 "
                  "order by h.start_time"
                  ,
        result);
    ASSERT_EQ(1, result.size());
}

// TODO verify shirakami fix
TEST_F(validate_batch_test, DISABLED_self_read_after_update) {
    // test scenario coming from batch verify
    execute_statement("create table test (foo int, bar bigint, zzz varchar(10), primary key(foo))");
//    execute_statement("INSERT INTO test (foo, bar, zzz) VALUES (0,  0, '000')");

    auto tx = utils::create_transaction(*db_, false, false);
    execute_statement("INSERT INTO test (foo, bar, zzz) VALUES (123, 123, '123')", *tx);
    execute_statement("UPDATE test SET bar = 100 where foo = 123", *tx);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT foo, bar, zzz FROM test", *tx, result);
    EXPECT_EQ(1, result.size());
    ASSERT_EQ(status::ok, tx->commit());
}
}
