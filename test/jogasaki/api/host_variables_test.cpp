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
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/configuration.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/datastore/blob_pool_mock.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/time_of_day_field_option.h>
#include <jogasaki/meta/time_point_field_option.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/create_file.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

#include <jogasaki/api/transaction_handle_internal.h>

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;

using takatori::util::unsafe_downcast;

class host_variables_test :
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
        auto* impl = db_impl();
        utils::add_test_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
    void test_invalid_parameter_types(
        std::string_view type, api::field_type_kind variable_type,
        std::function<void(api::parameter_set &)> set_parameter);
};

using namespace std::string_view_literals;
using kind = meta::field_type_kind;

TEST_F(host_variables_test, insert_basic) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
    };
    auto ps = api::create_parameter_set();
    ps->set_int8("p0", 1);
    ps->set_float8("p1", 10.0);
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (:p0, :p1)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(10.0, result[0].get_value<double>(1));
}

TEST_F(host_variables_test, update_basic) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
        {"i0", api::field_type_kind::int8},
        {"i1", api::field_type_kind::int8},
    };
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");

    {
        auto ps = api::create_parameter_set();
        ps->set_int8("p0", 1);
        ps->set_float8("p1", 20.0);
        execute_statement( "UPDATE T0 SET C1 = :p1 WHERE C0 = :p0", variables, *ps);

        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
        EXPECT_DOUBLE_EQ(20.0, result[0].get_value<double>(1));
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_int8("i0", 1);
        ps->set_int8("i1", 2);
        execute_statement( "UPDATE T0 SET C0 = :i1 WHERE C0 = :i0", variables, *ps);
        wait_epochs(2);

        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(2, result[0].get_value<std::int64_t>(0));
        EXPECT_DOUBLE_EQ(20.0, result[0].get_value<double>(1));
    }
}

TEST_F(host_variables_test, query_basic) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
    };
    auto ps = api::create_parameter_set();
    ps->set_int8("p0", 1);
    ps->set_float8("p1", 10.0);
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (:p0, :p1)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 WHERE C0 = :p0 AND C1 = :p1", variables, *ps, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(10.0, result[0].get_value<double>(1));
}

TEST_F(host_variables_test, range_scan_with_host_variables) {
    execute_statement("create table t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (0),(1),(2)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
        {"p1", api::field_type_kind::int4},
    };
    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 0);
    ps->set_int4("p1", 2);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t WHERE :p0 < c0 AND c0 < :p1", variables, *ps, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4>(1)), result[0]);
}

TEST_F(host_variables_test, insert_varieties_of_types) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
        {"p1", api::field_type_kind::int8},
        {"p2", api::field_type_kind::float8},
        {"p3", api::field_type_kind::float4},
        {"p4", api::field_type_kind::character},
    };
    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_int8("p1", 10);
    ps->set_float8("p2", 100.0);
    ps->set_float4("p3", 1000.0);
    ps->set_character("p4", "10000");
    execute_statement( "INSERT INTO T1 (C0, C1, C2, C3, C4) VALUES (:p0, :p1, :p2, :p3, :p4)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T1", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::typed_nullable_record<kind::int4, kind::int8, kind::float8, kind::float4, kind::character>(
        std::tuple{int4_type(), int8_type(), float8_type(), float4_type(), character_type(true, 100)},
        std::forward_as_tuple(1, 10, 100.0, 1000.0, accessor::text{"10000"}))), result[0]);
}

TEST_F(host_variables_test, update_varieties_of_types) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
        {"p1", api::field_type_kind::int8},
        {"p2", api::field_type_kind::float8},
        {"p3", api::field_type_kind::float4},
        {"p4", api::field_type_kind::character},
    };
    execute_statement( "INSERT INTO T1 (C0, C1, C2, C3, C4) VALUES (1, 10, 100.0, 1000.0, '10000')");
    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 2);
        ps->set_int8("p1", 20);
        ps->set_float8("p2", 200.0);
        ps->set_float4("p3", 2000.0);
        ps->set_character("p4", "20000");
        execute_statement( "UPDATE T1 SET C0 = :p0, C1 = :p1, C2 = :p2, C3 = :p3, C4 = :p4 WHERE C0 = 1", variables, *ps);

        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::int4, kind::int8, kind::float8, kind::float4, kind::character>(
            std::tuple{int4_type(), int8_type(), float8_type(), float4_type(), character_type(true, 100)},
            std::forward_as_tuple(2, 20, 200.0, 2000.0, accessor::text{"20000"}))), result[0]);
    }
    execute_statement( "DELETE FROM T1");
    execute_statement( "INSERT INTO T1 (C0, C1, C2, C3, C4) VALUES (1, 10, 100.0, 1000.0, '10000')");
    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 1);
        ps->set_int8("p1", 10);
        ps->set_float8("p2", 100.0);
        ps->set_float4("p3", 1000.0);
        ps->set_character("p4", "10000");
        execute_statement( "UPDATE T1 SET C0 = 2 WHERE C0 = :p0 AND C1 = :p1 AND C2 = :p2 AND C3 = :p3 AND C4 = :p4", variables, *ps);

        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T1", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::int4, kind::int8, kind::float8, kind::float4, kind::character>(
            std::tuple{int4_type(), int8_type(), float8_type(), float4_type(), character_type(true, 100)},
            std::forward_as_tuple(2, 10, 100.0, 1000.0, accessor::text{"10000"}))), result[0]);
    }
}

TEST_F(host_variables_test, query_varieties_of_types) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int4},
        {"p1", api::field_type_kind::int8},
        {"p2", api::field_type_kind::float8},
        {"p3", api::field_type_kind::float4},
        {"p4", api::field_type_kind::character},
    };
    execute_statement( "INSERT INTO T1 (C0, C1, C2, C3, C4) VALUES (1, 10, 100.0, 1000.0, '10000')");
    {
        auto ps = api::create_parameter_set();
        ps->set_int4("p0", 1);
        ps->set_int8("p1", 10);
        ps->set_float8("p2", 100.0);
        ps->set_float4("p3", 1000.0);
        ps->set_character("p4", "10000");
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T1 WHERE C0 = :p0 AND C1 = :p1 AND C2 = :p2 AND C3 = :p3 AND C4 = :p4", variables, *ps, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::typed_nullable_record<kind::int4, kind::int8, kind::float8, kind::float4, kind::character>(
            std::tuple{int4_type(), int8_type(), float8_type(), float4_type(), character_type(true, 100)},
            std::forward_as_tuple(1, 10, 100.0, 1000.0, accessor::text{"10000"}))), result[0]);
    }
}

TEST_F(host_variables_test, insert_temporal_types) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::date},
        {"p1", api::field_type_kind::time_of_day},
        {"p2", api::field_type_kind::time_of_day_with_time_zone},
        {"p3", api::field_type_kind::time_point},
        {"p4", api::field_type_kind::time_point_with_time_zone},
    };
    auto d2000_1_1 = date_v{2000, 1, 1};
    auto t12_0_0 = time_of_day_v{12, 0, 0};
    auto tp2000_1_1_12_0_0 = time_point_v{d2000_1_1, t12_0_0};
    auto ps = api::create_parameter_set();
    ps->set_date("p0", d2000_1_1);
    ps->set_time_of_day("p1", t12_0_0);
    ps->set_time_of_day("p2", t12_0_0);
    ps->set_time_point("p3", tp2000_1_1_12_0_0);
    ps->set_time_point("p4", tp2000_1_1_12_0_0);
    execute_statement( "INSERT INTO TTEMPORALS (K0, K1, K2, K3, K4, C0, C1, C2, C3, C4) VALUES (:p0, :p1, :p2, :p3, :p4, :p0, :p1, :p2, :p3, :p4)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM TTEMPORALS", result);
    ASSERT_EQ(1, result.size());

    auto dat = meta::field_type{meta::field_enum_tag<kind::date>};
    auto tod = meta::field_type{std::make_shared<meta::time_of_day_field_option>(false)};
    auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
    auto todtz = meta::field_type{std::make_shared<meta::time_of_day_field_option>(true)};
    auto tptz = meta::field_type{std::make_shared<meta::time_point_field_option>(true)};
    EXPECT_EQ((mock::typed_nullable_record<
        kind::date, kind::time_of_day, kind::time_of_day, kind::time_point, kind::time_point,
        kind::date, kind::time_of_day, kind::time_of_day, kind::time_point, kind::time_point
    >(
        std::tuple{
            dat, tod, todtz, tp, tptz,
            dat, tod, todtz, tp, tptz,
        },
        {
            d2000_1_1, t12_0_0, t12_0_0, tp2000_1_1_12_0_0, tp2000_1_1_12_0_0,
            d2000_1_1, t12_0_0, t12_0_0, tp2000_1_1_12_0_0, tp2000_1_1_12_0_0,
        }
    )), result[0]);
}

TEST_F(host_variables_test, update_temporal_types) {
    auto dat = meta::field_type{meta::field_enum_tag<kind::date>};
    auto tod = meta::field_type{std::make_shared<meta::time_of_day_field_option>(false)};
    auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
    auto todtz = meta::field_type{std::make_shared<meta::time_of_day_field_option>(true)};
    auto tptz = meta::field_type{std::make_shared<meta::time_point_field_option>(true)};

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::date},
        {"p1", api::field_type_kind::time_of_day},
        {"p2", api::field_type_kind::time_of_day_with_time_zone},
        {"p3", api::field_type_kind::time_point},
        {"p4", api::field_type_kind::time_point_with_time_zone},

        {"n0", api::field_type_kind::date},
        {"n1", api::field_type_kind::time_of_day},
        {"n2", api::field_type_kind::time_of_day_with_time_zone},
        {"n3", api::field_type_kind::time_point},
        {"n4", api::field_type_kind::time_point_with_time_zone},
    };
    auto d2000_1_1 = date_v{2000, 1, 1};
    auto t12_0_0 = time_of_day_v{12, 0, 0};
    auto tp2000_1_1_12_0_0 = time_point_v{d2000_1_1, t12_0_0};
    auto d2000_2_2 = date_v{2000, 2, 2};
    auto t12_2_2 = time_of_day_v{12, 2, 2};
    auto tp2000_2_2_12_2_2 = time_point_v{d2000_2_2, t12_2_2};
    {
        auto ps = api::create_parameter_set();
        ps->set_date("p0", d2000_1_1);
        ps->set_time_of_day("p1", t12_0_0);
        ps->set_time_of_day("p2", t12_0_0);
        ps->set_time_point("p3", tp2000_1_1_12_0_0);
        ps->set_time_point("p4", tp2000_1_1_12_0_0);
        execute_statement( "INSERT INTO TTEMPORALS (K0, K1, K2, K3, K4, C0, C1, C2, C3, C4) VALUES (:p0, :p1, :p2, :p3, :p4, :p0, :p1, :p2, :p3, :p4)", variables, *ps);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TTEMPORALS", result);
        ASSERT_EQ(1, result.size());
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_date("p0", d2000_1_1);
        ps->set_time_of_day("p1", t12_0_0);
        ps->set_time_of_day("p2", t12_0_0);
        ps->set_time_point("p3", tp2000_1_1_12_0_0);
        ps->set_time_point("p4", tp2000_1_1_12_0_0);

        ps->set_date("n0", d2000_2_2);
        ps->set_time_of_day("n1", t12_2_2);
        ps->set_time_of_day("n2", t12_2_2);
        ps->set_time_point("n3", tp2000_2_2_12_2_2);
        ps->set_time_point("n4", tp2000_2_2_12_2_2);
        execute_statement( "UPDATE TTEMPORALS SET C0 = :n0, C1 = :n1, C2 = :n2, C3 = :n3, C4 = :n4 WHERE K0 = :p0 AND K1 = :p1 AND K2 = :p2 AND K3 = :p3 AND K4 = :p4", variables, *ps);

        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TTEMPORALS", result);
        ASSERT_EQ(1, result.size());

        EXPECT_EQ((mock::typed_nullable_record<
            kind::date, kind::time_of_day, kind::time_of_day, kind::time_point, kind::time_point,
            kind::date, kind::time_of_day, kind::time_of_day, kind::time_point, kind::time_point
        >(
            std::tuple{
                dat, tod, todtz, tp, tptz,
                dat, tod, todtz, tp, tptz,
            },
            {
                d2000_1_1, t12_0_0, t12_0_0, tp2000_1_1_12_0_0, tp2000_1_1_12_0_0,
                d2000_2_2, t12_2_2, t12_2_2, tp2000_2_2_12_2_2, tp2000_2_2_12_2_2,
            }
        )), result[0]);
    }
}

TEST_F(host_variables_test, insert_decimal_types) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal},
        {"p2", api::field_type_kind::decimal},
    };
    auto ps = api::create_parameter_set();
    auto v111 = decimal_v{1, 0, 111, 0}; // 111
    auto v11_111 = decimal_v{1, 0, 11111, -3}; // 11.111
    auto v11111_1 = decimal_v{1, 0, 111111, -1}; // 11111.1

    ps->set_decimal("p0", v111);
    ps->set_decimal("p1", v11_111);
    ps->set_decimal("p2", v11111_1);
    execute_statement( "INSERT INTO TDECIMALS (K0, K1, K2, C0, C1, C2) VALUES (:p0, :p1, :p2, :p0, :p1, :p2)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM TDECIMALS", result);
    ASSERT_EQ(1, result.size());

    auto dec_3_0 = meta::field_type{std::make_shared<meta::decimal_field_option>(3, 0)};
    auto dec_5_3 = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
    auto dec_10_1 = meta::field_type{std::make_shared<meta::decimal_field_option>(10, 1)};
    EXPECT_EQ((mock::typed_nullable_record<
        kind::decimal, kind::decimal, kind::decimal,
        kind::decimal, kind::decimal, kind::decimal
    >(
        std::tuple{
            dec_3_0, dec_5_3, dec_10_1,
            dec_3_0, dec_5_3, dec_10_1,
        },
        {
            v111, v11_111, v11111_1,
            v111, v11_111, v11111_1,
        }
    )), result[0]);
}

TEST_F(host_variables_test, update_decimal_types) {
    auto dat = meta::field_type{meta::field_enum_tag<kind::date>};
    auto tod = meta::field_type{std::make_shared<meta::time_of_day_field_option>(false)};
    auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
        {"p1", api::field_type_kind::decimal},
        {"p2", api::field_type_kind::decimal},
        {"n0", api::field_type_kind::decimal},
        {"n1", api::field_type_kind::decimal},
        {"n2", api::field_type_kind::decimal},
    };
    auto v111 = decimal_v{1, 0, 111, 0}; // 111
    auto v11_111 = decimal_v{1, 0, 11111, -3}; // 11.111
    auto v11111_1 = decimal_v{1, 0, 111111, -1}; // 11111.1
    auto v222 = decimal_v{1, 0, 222, 0}; // 222
    auto v22_222 = decimal_v{1, 0, 22222, -3}; // 11.111
    auto v22222_2 = decimal_v{1, 0, 222222, -1}; // 11111.1
    {
        auto ps = api::create_parameter_set();
        ps->set_decimal("p0", v111);
        ps->set_decimal("p1", v11_111);
        ps->set_decimal("p2", v11111_1);
        execute_statement( "INSERT INTO TDECIMALS (K0, K1, K2, C0, C1, C2) VALUES (:p0, :p1, :p2, :p0, :p1, :p2)", variables, *ps);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TDECIMALS", result);
        ASSERT_EQ(1, result.size());
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_decimal("p0", v111);
        ps->set_decimal("p1", v11_111);
        ps->set_decimal("p2", v11111_1);

        ps->set_decimal("n0", v222);
        ps->set_decimal("n1", v22_222);
        ps->set_decimal("n2", v22222_2);
        execute_statement( "UPDATE TDECIMALS SET C0 = :n0, C1 = :n1, C2 = :n2 WHERE K0 = :p0 AND K1 = :p1 AND K2 = :p2", variables, *ps);

        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TDECIMALS", result);
        ASSERT_EQ(1, result.size());

        auto dec_3_0 = meta::field_type{std::make_shared<meta::decimal_field_option>(3, 0)};
        auto dec_5_3 = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
        auto dec_10_1 = meta::field_type{std::make_shared<meta::decimal_field_option>(10, 1)};
        EXPECT_EQ((mock::typed_nullable_record<
            kind::decimal, kind::decimal, kind::decimal,
            kind::decimal, kind::decimal, kind::decimal
        >(
            std::tuple{
                dec_3_0, dec_5_3, dec_10_1,
                dec_3_0, dec_5_3, dec_10_1,
            },
            {
                v111, v11_111, v11111_1,
                v222, v22_222, v22222_2,
            }
        )), result[0]);
    }
}

TEST_F(host_variables_test, cast_test) {
    execute_statement("create table TT (C0 int primary key, C1 bigint, C2 float, C3 double)");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character},
        {"p1", api::field_type_kind::character},
        {"p2", api::field_type_kind::character},
        {"p3", api::field_type_kind::character},
    };
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "1");
    ps->set_character("p1", "10");
    ps->set_character("p2", "100.0");
    ps->set_character("p3", "1000.0");
    execute_statement("INSERT INTO TT (C0, C1, C2, C3) VALUES (CAST(:p0 AS INT), CAST(:p1 AS BIGINT), CAST(:p2 AS FLOAT), CAST(:p3 AS DOUBLE))", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1, C2, C3 FROM TT", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::float4, kind::float8>({1, 10, 100.0, 1000.0}, {false, false, false, false})), result[0]);
    }
}

TEST_F(host_variables_test, cast_decimals) {
    execute_statement("create table TT (C0 decimal(5,3) primary key, C1 decimal(4,1), C2 decimal(10))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::character},
        {"p1", api::field_type_kind::character},
        {"p2", api::field_type_kind::character},
        {"p3", api::field_type_kind::character},
    };
    auto ps = api::create_parameter_set();
    ps->set_character("p0", "12.345");
    ps->set_character("p1", "123.4");
    ps->set_character("p2", "1234567890");
    execute_statement("INSERT INTO TT (C0, C1, C2) VALUES (CAST(:p0 AS DECIMAL(*,*)), CAST(:p1 AS DECIMAL(*,*)), CAST(:p2 AS DECIMAL(*,*)))", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1, C2 FROM TT", result);
        ASSERT_EQ(1, result.size());
        auto dec_5_3 = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
        auto dec_4_1 = meta::field_type{std::make_shared<meta::decimal_field_option>(4, 1)};
        auto dec_10 = meta::field_type{std::make_shared<meta::decimal_field_option>(10, 0)};
        auto v12_345 = decimal_v{1, 0, 12345, -3};
        auto v123_4 = decimal_v{1, 0, 1234, -1};
        auto v1234567890 = decimal_v{1, 0, 1234567890, 0};
        EXPECT_EQ((mock::typed_nullable_record<
            kind::decimal, kind::decimal, kind::decimal
        >(
            std::tuple{
                dec_5_3, dec_4_1, dec_10
            },
            {
                v12_345, v123_4, v1234567890
            }
        )), result[0]);
    }
}

TEST_F(host_variables_test, cast_inexact_decimals) {
    execute_statement("create table TT (C0 decimal(4,3))");
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::decimal},
    };
    auto v10 = decimal_v{1, 0, 10, 0};
    auto ps = api::create_parameter_set();
    ps->set_decimal("p0", v10);
    execute_statement("INSERT INTO TT (C0) VALUES (:p0)", variables, *ps, status::err_expression_evaluation_failure);
    execute_statement("INSERT INTO TT (C0) VALUES (:p0/3)", variables, *ps, status::err_expression_evaluation_failure);
    execute_statement("INSERT INTO TT (C0) VALUES (CAST(:p0/3 AS DECIMAL(4,3)))", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT", result);
        ASSERT_EQ(1, result.size());
        auto dec_4_3 = meta::field_type{std::make_shared<meta::decimal_field_option>(4, 3)};
        auto v3_333 = decimal_v{1, 0, 3333, -3};
        EXPECT_EQ((mock::typed_nullable_record<kind::decimal>(std::tuple{dec_4_3}, {v3_333})), result[0]);
    }
}

TEST_F(host_variables_test, missing_colon) {
    // before moving to new sql compiler, jogasaki wrongly passed host variables definition also as user/system
    // variables. So host variables worked even without colon. This test is to check that it is fixed.
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int8},
            {"p1", api::field_type_kind::float8},
    };
    auto ps = api::create_parameter_set();
    ps->set_int8("p0", 1);
    ps->set_float8("p1", 10.0);
    test_stmt_err( "INSERT INTO T0 (C0, C1) VALUES (p0, p1)", variables, *ps, error_code::symbol_analyze_exception);
}

TEST_F(host_variables_test, missing_parameter) {
    // valid type used by host variables (placeholders), but no value is set for the parameter
    execute_statement("create table t (c0 int primary key, c1 int)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::int4},
        };

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);

    test_stmt_err("INSERT INTO t VALUES (:p0, :p1)", variables, *ps, error_code::unresolved_placeholder_exception);
}

void host_variables_test::test_invalid_parameter_types(
    std::string_view type,
    api::field_type_kind variable_type,
    std::function<void(api::parameter_set&)> set_parameter
) {
    // valid type used by host variables (placeholders), but invalid type is used for parameters
    execute_statement("create table t (c0 int primary key, c1 "+std::string{type}+")");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", variable_type},
        };

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    set_parameter(*ps);

    test_stmt_err("INSERT INTO t VALUES (:p0, :p1)", variables, *ps, error_code::parameter_exception);
}

TEST_F(host_variables_test, invalid_parameter_types_octet_for_char) {
    test_invalid_parameter_types("varbinary", api::field_type_kind::octet, [](auto&& ps) {
        ps.set_character("p1", "ABC"sv);
    });
}

TEST_F(host_variables_test, invalid_parameter_types_double_for_int) {
    test_invalid_parameter_types("int", api::field_type_kind::int4, [](auto&& ps) {
        ps.set_float8("p1", 1.1);
    });
}

TEST_F(host_variables_test, invalid_parameter_types_float4_for_float8) {
    test_invalid_parameter_types("double", api::field_type_kind::float8, [](auto&& ps) {
        ps.set_float4("p1", 10.0);
    });
}

TEST_F(host_variables_test, admissible_parameter_types_int4_for_int8) {
    // int4 can be used for int8
    execute_statement("create table t (c0 int primary key, c1 bigint)");
    std::unordered_map<std::string, api::field_type_kind> variables{
            {"p0", api::field_type_kind::int4},
            {"p1", api::field_type_kind::int8},
        };

    auto ps = api::create_parameter_set();
    ps->set_int4("p0", 1);
    ps->set_int4("p1", 10000);

    execute_statement("INSERT INTO t VALUES (:p0, :p1)", variables, *ps);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT c1 FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8>(10000)), result[0]);
    }
}

}
