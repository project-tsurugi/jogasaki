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

class unsupported_sql_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->enable_index_join(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(unsupported_sql_test, join_scan) {
    execute_statement("create table T ("
                      "C0 int not null,"
                      "C1 int not null,"
                      "primary key (C0, C1)"
                      ")"
    );
    execute_statement("INSERT INTO T VALUES (1, 20220505)");

    execute_statement("create table S ("
                      "C0 int not null,"
                      "C1 int not null,"
                      "C2 int,"
                      "primary key (C0, C1)"
                      ")"
    );
    execute_statement("INSERT INTO S VALUES (1, 20220101, 20221231)");
    test_stmt_err(
        "select * from T inner join S on T.C0 = S.C0 "
        "where S.C1 < T.C1 "
        "and T.C1 < S.C2 "
        "and S.C0 = 1",
//        "and S.C0 = 1 and S.C1 = 2",  // specifying S.C1 results in join_find
        error_code::unsupported_runtime_feature_exception,
        "Compiling statement resulted in unsupported relational operator. Specify configuration parameter enable_index_join=false to avoid this."
    );
}

TEST_F(unsupported_sql_test, ddl_with_binary_type) {
    test_stmt_err(
        "create table T ("
        "C0 INT NOT NULL PRIMARY KEY,"
        "C1 binary(10)"
        ")",
        error_code::unsupported_runtime_feature_exception,
        "Data type specified for column \"C1\" is unsupported."
    );
}

TEST_F(unsupported_sql_test, ddl_with_varbinary_type) {
    test_stmt_err(
        "create table T ("
        "C0 INT NOT NULL PRIMARY KEY,"
        "C1 varbinary(10)"
        ")",
        error_code::unsupported_runtime_feature_exception,
        "Data type specified for column \"C1\" is unsupported."
    );
}

TEST_F(unsupported_sql_test, subquery) {
    execute_statement("create table T (C0 int not null primary key)");
    test_stmt_err(
        "select * from (select * from T t11, T t12) t1",
        error_code::compile_exception,
        "unexpected compile error occurred (likely unsupported SQL): must not be a join scope"
    );
}
}
