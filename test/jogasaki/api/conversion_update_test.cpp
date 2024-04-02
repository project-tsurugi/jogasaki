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

#include <cstddef>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <type_traits>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
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
using accessor::text;

using kind = meta::field_type_kind;

class conversion_update_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return true;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
    template <kind From, kind To>
    void test_update_between_types(std::string_view src, std::optional<runtime_t<To>> expected);

    template <kind From, kind To>
    void test_conversion_error(std::string_view src, error_code expected);

    template <kind From, kind To>
    void test_host_variable_update(runtime_t<From> src, runtime_t<To> expected);

    template <kind From, kind To>
    void test_host_variable_update_error(runtime_t<From> src, error_code expected);

    template <kind To>
    void test_setting_value_directly(std::string_view src, std::optional<runtime_t<To>> expected);
};

using namespace std::string_view_literals;

std::string type(kind k, bool from = true) {
    switch(k) {
        case kind::int1: return "tinyint";
        case kind::int2: return "smallint";
        case kind::int4: return "int";
        case kind::int8: return "bigint";
        case kind::float4: return "real";
        case kind::float8: return "double";
        case kind::decimal: return from ? "decimal(38,19)" : "decimal(10,5)";
        case kind::character: return "varchar(*)";
        case kind::date: return "date";
        case kind::time_of_day: return "time";
        case kind::time_point: return "timestamp";
    }
    std::abort();
}

template <kind From, kind To>
void conversion_update_test::test_update_between_types(std::string_view src, std::optional<runtime_t<To>> expected) {
    execute_statement("drop table if exists t");
    execute_statement("create table t (c0 "+type(To, false)+", c1 "+type(From, true)+")");
    execute_statement("INSERT INTO t VALUES (null, "+std::string{src}+")");
    execute_statement("UPDATE t SET c0 = c1");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t", result);
        ASSERT_EQ(1, result.size());
        if constexpr (To == kind::decimal) {
            if(expected.has_value()) {
                EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
                    std::tuple{decimal_type(10, 5)},
                    std::forward_as_tuple(expected.value()))), result[0]);
            } else {
                EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
                    std::tuple{decimal_type(10, 5)},
                    std::forward_as_tuple(triple{}),
                    {true})), result[0]);
            }
        } else {
            if(expected.has_value()) {
                EXPECT_EQ((create_nullable_record<To>(expected.value())), result[0]);
            } else {
                EXPECT_EQ((create_nullable_record<To>({}, {true})), result[0]);
            }
        }
    }
}

template <kind From, kind To>
void conversion_update_test::test_conversion_error(std::string_view src, error_code expected) {
    execute_statement("drop table if exists t");
    execute_statement("create table t (c0 "+type(To, false)+", c1 "+type(From, true)+")");
    execute_statement("INSERT INTO t VALUES (null, "+std::string{src}+")");
    test_stmt_err("UPDATE t SET c0 = c1", expected);
}

api::field_type_kind to_field_type_kind(kind k) {
    switch(k) {
        case kind::int1: return api::field_type_kind::int1;
        case kind::int2: return api::field_type_kind::int2;
        case kind::int4: return api::field_type_kind::int4;
        case kind::int8: return api::field_type_kind::int8;
        case kind::float4: return api::field_type_kind::float4;
        case kind::float8: return api::field_type_kind::float8;
        case kind::decimal: return api::field_type_kind::decimal;
        case kind::character: return api::field_type_kind::character;
        case kind::date: return api::field_type_kind::date;
        case kind::time_of_day: return api::field_type_kind::time_of_day;
        case kind::time_point: return api::field_type_kind::time_point;
    }
    std::abort();
}
template <kind Kind>
void set_value(api::parameter_set& ps, std::string_view name, runtime_t<Kind> value) {
    if constexpr (Kind == kind::int1) {
        ps.set_int4(name, value);
    } else if constexpr (Kind == kind::int2) {
        ps.set_int4(name, value);
    } else if constexpr (Kind == kind::int4) {
        ps.set_int4(name, value);
    } else if constexpr (Kind == kind::int8) {
        ps.set_int8(name, value);
    } else if constexpr (Kind == kind::float4) {
        ps.set_float4(name, value);
    } else if constexpr (Kind == kind::float8) {
        ps.set_float8(name, value);
    } else if constexpr (Kind == kind::decimal) {
        ps.set_decimal(name, value);
    } else if constexpr (Kind == kind::character) {
        ps.set_character(name, value);
    } else if constexpr (Kind == kind::date) {
        ps.set_date(name, value);
    } else if constexpr (Kind == kind::time_of_day) {
        ps.set_time_of_day(name, value);
    } else if constexpr (Kind == kind::time_point) {
        ps.set_time_point(name, value);
    }
}

template <kind From, kind To>
void conversion_update_test::test_host_variable_update(runtime_t<From> src, runtime_t<To> expected) {
    execute_statement("drop table if exists t");
    execute_statement("create table t (c0 "+type(To, false)+")");
    execute_statement("insert into t values (null)");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", to_field_type_kind(From)},
    };
    auto ps = api::create_parameter_set();
    set_value<From>(*ps, "p0"sv, src);
    execute_statement("UPDATE t SET c0 = :p0", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t", result);
        ASSERT_EQ(1, result.size());
        if constexpr (To == kind::decimal) {
            EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
                std::tuple{decimal_type(10, 5)},
                std::forward_as_tuple(expected))), result[0]);
        } else {
            EXPECT_EQ((create_nullable_record<To>(expected)), result[0]);
        }
    }
}

template <kind From, kind To>
void conversion_update_test::test_host_variable_update_error(runtime_t<From> src, error_code expected) {
    execute_statement("drop table if exists t");
    execute_statement("create table t (c0 "+type(To, false)+")");
    execute_statement("insert into t values (null)");

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", to_field_type_kind(From)},
    };
    auto ps = api::create_parameter_set();
    set_value<From>(*ps, "p0"sv, src);
    test_stmt_err("UPDATE t SET c0 = :p0", variables, *ps, expected);
}

TEST_F(conversion_update_test, int4_to_int8) {
    test_update_between_types<kind::int4, kind::int8>("1", 1);
    test_update_between_types<kind::int4, kind::int8>("-1", -1);
    test_host_variable_update<kind::int4, kind::int8>(-12, -12);
    test_update_between_types<kind::int4, kind::int8>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, int4_to_decimal) {
    test_update_between_types<kind::int4, kind::decimal>("1", 1);
    test_update_between_types<kind::int4, kind::decimal>("-1", -1);
    test_update_between_types<kind::int4, kind::decimal>("99999", 99999);
    test_conversion_error<kind::int4, kind::decimal>("100000", error_code::value_evaluation_exception);
    test_host_variable_update<kind::int4, kind::decimal>(-12, triple{-1,0,12, 0});
    test_update_between_types<kind::int4, kind::decimal>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, int4_to_float4) {
    test_update_between_types<kind::int4, kind::float4>("1", 1.0);
    test_update_between_types<kind::int4, kind::float4>("-1", -1.0);
    test_host_variable_update<kind::int4, kind::float4>(-12, -12.0);
    test_update_between_types<kind::int4, kind::float4>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, int4_to_float8) {
    test_update_between_types<kind::int4, kind::float8>("1", 1.0);
    test_update_between_types<kind::int4, kind::float8>("-1", -1.0);
    test_host_variable_update<kind::int4, kind::float8>(-12, -12.0);
    test_update_between_types<kind::int4, kind::float8>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, int8_to_int4) {
    test_update_between_types<kind::int8, kind::int4>("1", 1);
    test_update_between_types<kind::int8, kind::int4>("-1", -1);
    test_conversion_error<kind::int8, kind::int4>("2147483648", error_code::value_evaluation_exception);
    test_host_variable_update<kind::int8, kind::int4>(-12, -12);
    test_update_between_types<kind::int8, kind::int4>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, int8_to_decimal) {
    test_update_between_types<kind::int8, kind::decimal>("1", 1);
    test_update_between_types<kind::int8, kind::decimal>("-1", -1);
    test_update_between_types<kind::int8, kind::decimal>("99999", 99999);
    test_conversion_error<kind::int8, kind::decimal>("100000", error_code::value_evaluation_exception);
    test_host_variable_update<kind::int8, kind::decimal>(-12, triple{-1,0,12, 0});
    test_update_between_types<kind::int8, kind::decimal>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, int8_to_float4) {
    test_update_between_types<kind::int8, kind::float4>("1", 1.0);
    test_update_between_types<kind::int8, kind::float4>("-1", -1.0);
    test_host_variable_update<kind::int8, kind::float4>(-12, -12.0);
    test_update_between_types<kind::int8, kind::float4>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, int8_to_float8) {
    test_update_between_types<kind::int8, kind::float8>("1", 1.0);
    test_update_between_types<kind::int8, kind::float8>("-1", -1.0);
    test_host_variable_update<kind::int8, kind::float8>(-12, -12.0);
    test_update_between_types<kind::int8, kind::float8>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, decimal_to_int4) {
    test_update_between_types<kind::decimal, kind::int4>("CAST(1 AS DECIMAL(38,19))", 1);
    test_update_between_types<kind::decimal, kind::int4>("CAST(-1 AS DECIMAL(38,19))", -1);
    test_conversion_error<kind::decimal, kind::int4>("CAST(2147483648 AS DECIMAL(38,19))", error_code::value_evaluation_exception);
    test_host_variable_update<kind::decimal, kind::int4>(triple{-1,0,12, 0}, -12);
    test_update_between_types<kind::decimal, kind::int4>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, decimal_to_int8) {
    test_update_between_types<kind::decimal, kind::int8>("CAST(1 AS DECIMAL(38,19))", 1);
    test_update_between_types<kind::decimal, kind::int8>("CAST(-1 AS DECIMAL(38,19))", -1);
    test_conversion_error<kind::decimal, kind::int8>("CAST('9223372036854775808' AS DECIMAL(38,19))", error_code::value_evaluation_exception);
    test_host_variable_update<kind::decimal, kind::int8>(triple{-1,0,12,0}, -12);
    test_update_between_types<kind::decimal, kind::int8>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, decimal_to_decimal) {
    test_update_between_types<kind::decimal, kind::decimal>("CAST(1 AS DECIMAL(38,19))", 1);
    test_update_between_types<kind::decimal, kind::decimal>("CAST(-1 AS DECIMAL(38,19))", -1);
    test_host_variable_update<kind::decimal, kind::decimal>(triple{-1,0,12,0}, triple{-1,0,12,0});
    test_conversion_error<kind::decimal, kind::decimal>("CAST(100000 AS DECIMAL(38,19))", error_code::value_evaluation_exception);
    test_update_between_types<kind::decimal, kind::decimal>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, decimal_to_float4) {
    test_update_between_types<kind::decimal, kind::float4>("CAST(1 AS DECIMAL(38,19))", 1.0);
    test_update_between_types<kind::decimal, kind::float4>("CAST(-1 AS DECIMAL(38,19))", -1.0);
    test_host_variable_update<kind::decimal, kind::float4>(triple{-1,0,12,0}, -12.0);
    test_update_between_types<kind::decimal, kind::float4>("NULL", std::nullopt);
    test_host_variable_update<kind::decimal, kind::float4>(triple{-1,0,1,-100}, -0.0); // underflow
    test_host_variable_update<kind::decimal, kind::float4>(triple{-1,0,1,100}, -std::numeric_limits<float>::infinity()); // overflow
}

TEST_F(conversion_update_test, decimal_to_float8) {
    test_update_between_types<kind::decimal, kind::float8>("CAST(1 AS DECIMAL(38,19))", 1.0);
    test_update_between_types<kind::decimal, kind::float8>("CAST(-1 AS DECIMAL(38,19))", -1.0);
    test_host_variable_update<kind::decimal, kind::float8>(triple{-1,0,12,0}, -12.0);
    test_update_between_types<kind::decimal, kind::float8>("NULL", std::nullopt);
    test_host_variable_update<kind::decimal, kind::float8>(triple{-1,0,1,-500}, -0.0); // underflow
    test_host_variable_update<kind::decimal, kind::float8>(triple{-1,0,1,500}, -std::numeric_limits<float>::infinity()); // overflow
}

TEST_F(conversion_update_test, float4_to_int4) {
    test_conversion_error<kind::float4, kind::int4>("1.0", error_code::unsupported_runtime_feature_exception);
    test_host_variable_update_error<kind::float4, kind::int4>(-12, error_code::unsupported_runtime_feature_exception);
    test_conversion_error<kind::float4, kind::int4>("NULL", error_code::unsupported_runtime_feature_exception);
}

TEST_F(conversion_update_test, float4_to_int8) {
    test_conversion_error<kind::float4, kind::int8>("1.0", error_code::unsupported_runtime_feature_exception);
    test_host_variable_update_error<kind::float4, kind::int8>(-12, error_code::unsupported_runtime_feature_exception);
    test_conversion_error<kind::float4, kind::int8>("NULL", error_code::unsupported_runtime_feature_exception);
}

TEST_F(conversion_update_test, float4_to_decimal) {
    test_conversion_error<kind::float4, kind::decimal>("1.0", error_code::unsupported_runtime_feature_exception);
    test_host_variable_update_error<kind::float4, kind::decimal>(-12, error_code::unsupported_runtime_feature_exception);
    test_conversion_error<kind::float4, kind::decimal>("NULL", error_code::unsupported_runtime_feature_exception);
}

TEST_F(conversion_update_test, float4_to_float8) {
    test_update_between_types<kind::float4, kind::float8>("1.0", 1.0);
    test_update_between_types<kind::float4, kind::float8>("-1.0", -1.0);
    test_host_variable_update<kind::float4, kind::float8>(-12.0, -12.0);
    test_update_between_types<kind::float4, kind::float8>("NULL", std::nullopt);
}

TEST_F(conversion_update_test, float8_to_int4) {
    test_conversion_error<kind::float8, kind::int4>("1.0", error_code::unsupported_runtime_feature_exception);
    test_host_variable_update_error<kind::float8, kind::int4>(-12.0, error_code::unsupported_runtime_feature_exception);
    test_conversion_error<kind::float8, kind::int4>("NULL", error_code::unsupported_runtime_feature_exception);
}

TEST_F(conversion_update_test, float8_to_int8) {
    test_conversion_error<kind::float8, kind::int8>("1.0", error_code::unsupported_runtime_feature_exception);
    test_host_variable_update_error<kind::float8, kind::int8>(-12.0, error_code::unsupported_runtime_feature_exception);
    test_conversion_error<kind::float8, kind::int8>("NULL", error_code::unsupported_runtime_feature_exception);
}

TEST_F(conversion_update_test, float8_to_decimal) {
    test_conversion_error<kind::float8, kind::decimal>("1.0", error_code::unsupported_runtime_feature_exception);
    test_host_variable_update_error<kind::float8, kind::decimal>(-12.0, error_code::unsupported_runtime_feature_exception);
    test_conversion_error<kind::float8, kind::decimal>("NULL", error_code::unsupported_runtime_feature_exception);
}

TEST_F(conversion_update_test, float8_to_float4) {
    test_update_between_types<kind::float8, kind::float4>("1.0", 1.0);
    test_update_between_types<kind::float8, kind::float4>("-1.0", -1.0);
    test_update_between_types<kind::float8, kind::float4>("CAST('3.4029e+38' AS DOUBLE)", std::numeric_limits<float>::infinity());  // FLT_MAX + alpha
    test_update_between_types<kind::float8, kind::float4>("CAST('1.17549e-38' AS DOUBLE)", 0.0F);  // FLT_MIN - alpha
    test_host_variable_update<kind::float8, kind::float4>(-12.0, -12.0);
    test_update_between_types<kind::float8, kind::float4>("NULL", std::nullopt);
}

template <kind To>
void conversion_update_test::test_setting_value_directly(std::string_view src, std::optional<runtime_t<To>> expected) {
    execute_statement("drop table if exists t");
    execute_statement("create table t (c0 "+type(To, false)+")");
    execute_statement("INSERT INTO t VALUES (NULL)");
    execute_statement("UPDATE t SET c0 = "+std::string{src});
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c0 FROM t", result);
        ASSERT_EQ(1, result.size());
        if constexpr (To == kind::decimal) {
            if(expected.has_value()) {
                EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
                    std::tuple{decimal_type(10, 5)},
                    std::forward_as_tuple(expected.value()))), result[0]);
            } else {
                EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(
                    std::tuple{decimal_type(10, 5)},
                    std::forward_as_tuple(triple{}),
                    {true}
                    )), result[0]);
            }
        } else {
            if(expected.has_value()) {
                EXPECT_EQ((create_nullable_record<To>(expected.value())), result[0]);
            } else {
                EXPECT_EQ((create_nullable_record<To>({}, {true})), result[0]);
            }
        }
    }
}

TEST_F(conversion_update_test, null) {
    // verify assignment conversion from `unknown` type
    test_setting_value_directly<kind::int4>("NULL", std::nullopt);
    test_setting_value_directly<kind::int8>("NULL", std::nullopt);
    test_setting_value_directly<kind::float4>("NULL", std::nullopt);
    test_setting_value_directly<kind::float8>("NULL", std::nullopt);
    test_setting_value_directly<kind::decimal>("NULL", std::nullopt);
}

}  // namespace jogasaki::testing
