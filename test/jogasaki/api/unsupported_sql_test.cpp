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

#include <memory>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/type_helper.h>
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

using kind = meta::field_type_kind;

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
    // join_scan is not implemented, but the compiler fallbacks to shuffle join then
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
    std::vector<mock::basic_record> result{};
    execute_query(
        "select * from T inner join S on T.C0 = S.C0 "
        "where S.C1 < T.C1 "
        "and T.C1 < S.C2 "
        "and S.C0 = 1 ",
       // "and S.C0 = 1 and S.C1 = 2",  // specifying S.C1 results in join_find
        result
    );
    ASSERT_EQ(1, result.size());
}

TEST_F(unsupported_sql_test, ddl_with_binary_type) {
    execute_statement(
        "create table T ("
        "C0 INT NOT NULL PRIMARY KEY,"
        "C1 binary(3),"
        "C2 varbinary(3)"
        ")"
    );
    execute_statement("INSERT INTO T VALUES (1, CAST('010203' AS BINARY(3)), CAST('010203' AS VARBINARY(3)))");

    std::vector<mock::basic_record> result{};
    execute_query("SELECT C1, C2 FROM T ORDER BY T.C0, T.C1", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::octet, kind::octet>(
        std::tuple{meta::octet_type(false, 3), meta::octet_type(true, 3)},
        std::forward_as_tuple(accessor::binary{"\x01\x02\x03"}, accessor::binary{"\x01\x02\x03"}))), result[0]);
}

TEST_F(unsupported_sql_test, ddl_with_varbinary_type_in_pk) {
    test_stmt_err(
        "create table T ("
        "C0 varbinary(10) NOT NULL PRIMARY KEY,"
        "C1 int"
        ")",
        error_code::unsupported_runtime_feature_exception,
        "data type used for column \"C0\" is unsupported for primary/secondary index key"
    );
}

TEST_F(unsupported_sql_test, ddl_with_binary_type_in_pk) {
    execute_statement("create table T ("
                      "C0 binary(10) NOT NULL PRIMARY KEY,"
                      "C1 int"
                      ")");
}

TEST_F(unsupported_sql_test, ddl_with_varbinary_type_in_secondary_index) {
    execute_statement("create table T (C0 INT PRIMARY KEY, C1 varbinary(10) NOT NULL)");
    test_stmt_err(
        "create index I on T (C1)",
        error_code::unsupported_runtime_feature_exception,
        "data type used for column \"C1\" is unsupported for primary/secondary index key"
    );
}

TEST_F(unsupported_sql_test, ddl_with_binary_type_in_secondary_index) {
    execute_statement("create table T (C0 INT PRIMARY KEY, C1 binary(10) NOT NULL)");
    execute_statement("create index I on T (C1)");
}


TEST_F(unsupported_sql_test, subquery) {
    // new compiler now supports subqueries
    execute_statement("create table T (C0 int not null primary key)");
    execute_statement("select * from (select * from T t11, T t12) t1");
}

TEST_F(unsupported_sql_test, aggregate_with_and_without_distinct_keyword) {
    execute_statement("create table t (c0 int)");
    test_stmt_err("select count(c0), count(distinct c0) from t", error_code::unsupported_runtime_feature_exception);
}

}
