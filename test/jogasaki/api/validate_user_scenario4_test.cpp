/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <cstddef>
#include <memory>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/create_tx.h>

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

class validate_user_scenario4_test :
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

// regression test scenario tsurugi-issues/issues/86
TEST_F(validate_user_scenario4_test, phantom_with_ddl) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    for(std::size_t i=0; i < 20; ++i) {
        if (i != 0) {
            execute_statement("drop table test");
        }
        execute_statement("create table test(  foo int,  bar bigint,  zzz varchar(10))");
        execute_statement("insert into test(foo, bar, zzz)values(2, 2, '2')");
        execute_statement("delete from test where foo = 2");
        execute_statement("delete from test where foo = 2");
    }
}

}
