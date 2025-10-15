/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_null_and_unknown_type_test :
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
    bool uses_secondary(
        std::string_view query,
        api::parameter_set const& params,
        std::unordered_map<std::string, api::field_type_kind> const& variables
    );
};

using namespace std::string_view_literals;

TEST_F(sql_null_and_unknown_type_test, read_null) {
    execute_statement("CREATE TABLE T0 (C0 BIGINT PRIMARY KEY, C1 DOUBLE)");
    execute_statement("INSERT INTO T0(C0) VALUES (0)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>({0, 0.0}, {false, true})), result[0]);
    }
}

TEST_F(sql_null_and_unknown_type_test, select_null_literal) {
    // select NULL literal (this is unknown type)
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int primary key)");
    execute_statement("INSERT INTO T VALUES (1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, NULL FROM T", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::unknown>(std::tuple{1, '\0'}, {false, true})), result[0]);
    }
}

TEST_F(sql_null_and_unknown_type_test, select_null_host_variable) {
    // similar to select_null, but using host variable instead of NULL literal
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int primary key)");
    execute_statement("INSERT INTO T VALUES (1)");
    {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::unknown},
        };
        auto ps = api::create_parameter_set();
        ps->set_null("p0");

        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, :p0 FROM T", variables, *ps, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::unknown>(std::tuple{1, -1}, {false, true})), result[0]);
    }
}

TEST_F(sql_null_and_unknown_type_test, binary_expression) {
    // calculate expression using NULL literal and null host variable
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int primary key)");
    execute_statement("INSERT INTO T VALUES (1)");
    {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::unknown},
        };
        auto ps = api::create_parameter_set();
        ps->set_null("p0");

        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT 1+NULL, NULL+1, 1+:p0, :p0+1 FROM T", variables, *ps, result);
            ASSERT_EQ(1, result.size());
            // because literal "1" is of type int8, so by binary promotion, the result type is int8
            EXPECT_EQ((create_nullable_record<kind::int8, kind::int8, kind::int8, kind::int8>(std::tuple{-1, -1, -1, -1}, {true, true, true, true})), result[0]);
        }
        test_stmt_err("SELECT NULL+NULL FROM T", variables, *ps, error_code::type_analyze_exception);
        test_stmt_err("SELECT :p0+NULL FROM T", variables, *ps, error_code::type_analyze_exception);
        test_stmt_err("SELECT NULL+:p0 FROM T", variables, *ps, error_code::type_analyze_exception);
        test_stmt_err("SELECT :p0+:p0 FROM T", variables, *ps, error_code::type_analyze_exception);
    }
}

TEST_F(sql_null_and_unknown_type_test, compare_expression) {
    // do comparison using NULL literal and null host variable
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int primary key)");
    execute_statement("INSERT INTO T VALUES (1)");
    {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::unknown},
        };
        auto ps = api::create_parameter_set();
        ps->set_null("p0");

        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT 1 < NULL, NULL < 1, 1 < :p0, :p0 < 1 FROM T", variables, *ps, result);
            ASSERT_EQ(1, result.size());
            EXPECT_EQ((create_nullable_record<kind::boolean, kind::boolean, kind::boolean, kind::boolean>(std::tuple{-1, -1, -1, -1}, {true, true, true, true})), result[0]);
        }
        test_stmt_err("SELECT NULL < NULL FROM T", variables, *ps, error_code::unsupported_compiler_feature_exception);
        test_stmt_err("SELECT :p0 < NULL FROM T", variables, *ps, error_code::unsupported_compiler_feature_exception);
        test_stmt_err("SELECT NULL < :p0 FROM T", variables, *ps, error_code::unsupported_compiler_feature_exception);
        test_stmt_err("SELECT :p0 < :p0 FROM T", variables, *ps, error_code::unsupported_compiler_feature_exception);
    }
}

TEST_F(sql_null_and_unknown_type_test, find_by_null) {
    // try to find row by comparison with NULL literal and null host variable (should be empty)
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int primary key)");
    execute_statement("INSERT INTO T VALUES (1)");
    {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::unknown},
        };
        auto ps = api::create_parameter_set();
        ps->set_null("p0");
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT C0 FROM T WHERE C0 = NULL", variables, *ps, result);
            ASSERT_EQ(0, result.size());
        }
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT C0 FROM T WHERE C0 = :p0", variables, *ps, result);
            ASSERT_EQ(0, result.size());
        }
        {
            std::vector<mock::basic_record> result{};
            execute_query("SELECT C0 FROM T WHERE C0 <> :p0", variables, *ps, result);
            ASSERT_EQ(0, result.size());
        }
    }
}

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

bool sql_null_and_unknown_type_test::uses_secondary(
    std::string_view query,
    api::parameter_set const& params,
    std::unordered_map<std::string, api::field_type_kind> const& variables
) {
    std::string plan{};
    explain_statement(query, plan, params, variables);
    return contains(plan, "\"i1\"");
}

TEST_F(sql_null_and_unknown_type_test, find_by_null_from_secondary) {
    // using secondary index, try to find row by comparison with NULL literal and null host variable (should be empty)
    utils::set_global_tx_option(utils::create_tx_option{false, false});
    execute_statement("create table T (C0 int primary key, C1 int)");
    execute_statement("create index i1 on T (C1)");
    execute_statement("INSERT INTO T VALUES (1, 1)");
    execute_statement("INSERT INTO T VALUES (2, NULL)");
    {
        std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::unknown},
        };
        auto ps = api::create_parameter_set();
        ps->set_null("p0");
        {
            std::vector<mock::basic_record> result{};
            auto query = "SELECT C0 FROM T WHERE C1 = NULL"sv;
            ASSERT_TRUE(uses_secondary(query, *ps, variables));
            execute_query(query, variables, *ps, result);
            ASSERT_EQ(0, result.size());
        }
        {
            std::vector<mock::basic_record> result{};
            auto query = "SELECT C0 FROM T WHERE C1 = :p0"sv;
            ASSERT_TRUE(uses_secondary(query, *ps, variables));
            execute_query(query, variables, *ps, result);
            ASSERT_EQ(0, result.size());
        }
        {
            std::vector<mock::basic_record> result{};
            auto query = "SELECT C0 FROM T WHERE C1 <> :p0"sv;
            // negating the condition won't use secondary - simply check the behavior anyway
            ASSERT_TRUE(! uses_secondary(query, *ps, variables));
            execute_query(query, variables, *ps, result);
            ASSERT_EQ(0, result.size());
        }
    }
}

}
