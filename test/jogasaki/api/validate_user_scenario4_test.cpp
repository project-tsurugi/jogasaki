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
        cfg->enable_index_join(false); // to workaround problem caused by join_scan not implemented yet //TODO
//        cfg->single_thread(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

// regression test scenario tsurugi-issues/issues/86
TEST_F(validate_user_scenario4_test, DISABLED_phantom_with_ddl) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    for(std::size_t i=0; i < 30; ++i) {
        if (i != 0) {
            execute_statement("drop table test");
        }
        execute_statement("create table test(  foo int,  bar bigint,  zzz varchar(10))");
        execute_statement("insert into test(foo, bar, zzz)values(0, 0, '0')");
        execute_statement("insert into test(foo, bar, zzz)values(1, 1, '1')");
        execute_statement("insert into test(foo, bar, zzz)values(2, 2, '2')");
        execute_statement("insert into test(foo, bar, zzz)values(3, 3, '3')");
        execute_statement("delete from test where foo = 2");
        execute_statement("delete from test where foo = 2");
        {
            std::vector<mock::basic_record> result{};
            execute_query(
                "select foo, bar, zzz from test order by foo, bar, zzz",
                result);
            ASSERT_EQ(3, result.size());
        }
    }
}

}
