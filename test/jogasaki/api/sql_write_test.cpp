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

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/data/any.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

// write related error handling testcases
class sql_write_test :
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

TEST_F(sql_write_test, expression_error_handling_with_update) {
    // verify transaction is aborted and rollbacked any changes on expression error
    execute_statement("CREATE TABLE T(C0 DECIMAL(5,3) NOT NULL)");
    auto v10 = decimal_v{1, 0, 10, 0}; // 10
    {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::decimal},
        };
        auto ps = api::create_parameter_set();
        ps->set_decimal("p0", v10);
        execute_statement("INSERT INTO T VALUES (:p0)", variables, *ps);
    }

    auto tx = utils::create_transaction(*db_);
    auto ps = api::create_parameter_set();
    execute_statement("UPDATE T SET C0 = C0 / 3", {}, *ps, *tx, status::err_expression_evaluation_failure);
    EXPECT_EQ(status::err_inactive_transaction, tx->commit());

    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 FROM T", result);
    ASSERT_EQ(1, result.size());

    auto dec = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(std::tuple{dec}, {v10})), result[0]);
}

TEST_F(sql_write_test, expression_error_handling_with_insert) {
    // verify transaction is aborted and rollbacked any changes on expression error
    execute_statement("CREATE TABLE T(C0 DECIMAL(5,3) NOT NULL)");
    auto tx = utils::create_transaction(*db_);
    {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::decimal},
        };
        auto ps = api::create_parameter_set();
        auto v10 = decimal_v{1, 0, 10, 0}; // 10
        ps->set_decimal("p0", v10);
        execute_statement("INSERT INTO T VALUES (:p0)", variables, *ps, *tx);
    }

    auto ps = api::create_parameter_set();
    execute_statement("INSERT INTO T VALUES (1)", {}, *ps, *tx, status::err_expression_evaluation_failure);
    EXPECT_EQ(status::err_inactive_transaction, tx->commit());

    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't support recovery";
    }
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0 FROM T", result);
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_write_test, pk_update_failure) {
    // verify updating pk record by record and hits unique constraint violation
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (0, 0)");
    execute_statement("INSERT INTO T VALUES (1, 1)");

    auto tx = utils::create_transaction(*db_);

    auto ps = api::create_parameter_set();
    execute_statement("UPDATE T SET C0 = C0 + 1", {}, *ps, *tx, status::err_unique_constraint_violation);
    EXPECT_EQ(status::err_inactive_transaction, tx->commit());

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(0, 0)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 1)), result[1]);
}

TEST_F(sql_write_test, pk_update_success) {
    // verify updating pk record by record where it doesn't hit unique constraint violation
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory goes into infinite loop when updating pk";
    }
    execute_statement("CREATE TABLE T(C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES (0, 0)");
    execute_statement("INSERT INTO T VALUES (2, 2)");

    auto tx = utils::create_transaction(*db_);

    auto ps = api::create_parameter_set();
    execute_statement("UPDATE T SET C0 = C0 + 1", {}, *ps, *tx);
    EXPECT_EQ(status::ok, tx->commit());

    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T ORDER BY C0", result);
    ASSERT_EQ(2, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(1, 0)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(3, 2)), result[1]);
}
}
