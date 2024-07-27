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

#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace std::chrono_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::decimal::triple;
using takatori::datetime::date;
using takatori::datetime::time_of_day;
using takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_decimal_types_test :
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

using jogasaki::accessor::text;

TEST_F(sql_decimal_types_test, insert_by_literal_cast_on_context) {
    execute_statement("CREATE TABLE T (C0 DECIMAL(3), C1 DECIMAL(5, 3))");
    execute_statement("INSERT INTO T VALUES ('1', '1')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal, kind::decimal>(
            std::tuple{
                meta::decimal_type(3, 0),
                meta::decimal_type(5, 3),
            }, {
                triple{1, 0, 1, 0},
                triple{1, 0, 1, 0},
            }
        )), result[0]);
    }
}

TEST_F(sql_decimal_types_test, length_unspecified_for_types) {
    execute_statement("CREATE TABLE T (C0 DECIMAL)");
    execute_statement("INSERT INTO T VALUES (123)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
            std::tuple{
                meta::decimal_type(38, 0),
            }, {
                triple{1, 0, 123, 0},
            }
        )), result[0]);
    }
}

TEST_F(sql_decimal_types_test, decimals_indefinitive_precscale) {
    execute_statement("CREATE TABLE TT(C0 DECIMAL(5,3) NOT NULL PRIMARY KEY)");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal}
    };
    auto ps = api::create_parameter_set();
    auto v1 = triple{1, 0, 1, 0}; // 1
    ps->set_decimal("p0", v1);
    execute_statement("INSERT INTO TT (C0) VALUES (:p0)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT C0*C0 as C0 FROM TT", result);
    ASSERT_EQ(1, result.size());
    auto& rec = result[0];
    EXPECT_FALSE(rec.is_null(0));

    EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type()}, {v1})), result[0]);
}

TEST_F(sql_decimal_types_test, store_double_literal_into_decimal) {
    // by analyzer option cast_literals_in_context = true, double literal is implicitly casted to decimal
    execute_statement("create table t (c0 decimal(5,3) primary key)");
    execute_statement("insert into t values (1.1e0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type(5, 3)}, {triple{1, 0, 11, -1}})),
            result[0]
        );
    }
    execute_statement("update t set c0 = 2.2");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(
            (mock::typed_nullable_record<kind::decimal>(std::tuple{decimal_type(5, 3)}, {triple{1, 0, 22, -1}})),
            result[0]
        );
    }

    // if the source is not literal, cast_literals_in_context doesn't apply and assignment conversion from double
    // to decimal is not allowed
    test_stmt_err("insert into t values (1.0e0+0.1e0)", error_code::unsupported_runtime_feature_exception);
    test_stmt_err("update t set c0 = 2.0e0+0.2e0", error_code::unsupported_runtime_feature_exception);
}

// TODO enable after fixing insufficient storage by encoder
TEST_F(sql_decimal_types_test, DISABLED_scan_by_longer_data) {
    // verify coder correctly distinguish runtime type and storage type
    // even if search key is longer than the column length, encode should be successful
    execute_statement("CREATE TABLE T (C0 DECIMAL(3), C1 DECIMAL(3), PRIMARY KEY(C0,C1))");
    execute_statement("INSERT INTO T VALUES (111, 111)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T WHERE C0 = 1234.56", result);
        ASSERT_EQ(0, result.size());
    }
}

// TODO enable after fixing insufficient storage by encoder
TEST_F(sql_decimal_types_test, DISABLED_find_by_longer_data) {
    // verify coder correctly distinguish runtime type and storage type
    // even if search key is longer than the column length, encode should be successful
    execute_statement("CREATE TABLE T (C0 DECIMAL(3), C1 DECIMAL(3), PRIMARY KEY(C0))");
    execute_statement("INSERT INTO T VALUES (111, 111)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T WHERE C0 = 1234.56", result);
        ASSERT_EQ(0, result.size());
    }
}

}  // namespace jogasaki::testing
