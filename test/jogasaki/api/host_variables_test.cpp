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

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
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

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;

using takatori::util::unsafe_downcast;

class host_variables_test :
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

        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
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
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::float8, kind::float4, kind::character>(1, 10, 100.0, 1000.0, accessor::text{"10000"})), result[0]);
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
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::float8, kind::float4, kind::character>(2, 20, 200.0, 2000.0, accessor::text{"20000"})), result[0]);
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
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::float8, kind::float4, kind::character>(2, 10, 100.0, 1000.0, accessor::text{"10000"})), result[0]);
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
        EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int8, kind::float8, kind::float4, kind::character>(1, 10, 100.0, 1000.0, accessor::text{"10000"})), result[0]);
    }
}

TEST_F(host_variables_test, insert_temporal_types) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::date},
        {"p1", api::field_type_kind::time_of_day},
        {"p2", api::field_type_kind::time_of_day}, //TODO with time zone
        {"p3", api::field_type_kind::time_point},
        {"p4", api::field_type_kind::time_point}, //TODO with time zone
    };
    auto ps = api::create_parameter_set();
    ps->set_date("p0", date_v{2000, 1, 1});
    ps->set_time_of_day("p1", time_of_day_v{12, 0, 0});
    ps->set_time_of_day("p2", time_of_day_v{12, 0, 0});
    ps->set_time_point("p3", time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}});
    ps->set_time_point("p4", time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}});
    execute_statement( "INSERT INTO TTEMPORALS (K0, K1, K2, K3, K4, C0, C1, C2, C3, C4) VALUES (:p0, :p1, :p2, :p3, :p4, :p0, :p1, :p2, :p3, :p4)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM TTEMPORALS", result);
    ASSERT_EQ(1, result.size());

    auto dat = meta::field_type{meta::field_enum_tag<kind::date>};
    auto tod = meta::field_type{std::make_shared<meta::time_of_day_field_option>(false)};
    auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
    EXPECT_EQ((mock::typed_nullable_record<
        kind::date, kind::time_of_day, kind::time_of_day, kind::time_point, kind::time_point,
        kind::date, kind::time_of_day, kind::time_of_day, kind::time_point, kind::time_point
    >(
        std::tuple{
            dat, tod, tod, tp, tp,
            dat, tod, tod, tp, tp,
        },
        {
            {date_v{2000, 1, 1}}, {time_of_day_v{12, 0, 0}}, {time_of_day_v{12, 0, 0}}, {time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}}}, {time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}}},
            {date_v{2000, 1, 1}}, {time_of_day_v{12, 0, 0}}, {time_of_day_v{12, 0, 0}}, {time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}}}, {time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}}},
        }
    )), result[0]);
}

TEST_F(host_variables_test, update_temporal_types) {
    auto dat = meta::field_type{meta::field_enum_tag<kind::date>};
    auto tod = meta::field_type{std::make_shared<meta::time_of_day_field_option>(false)};
    auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};

    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::date},
        {"p1", api::field_type_kind::time_of_day},
        {"p2", api::field_type_kind::time_of_day}, //TODO with time zone
        {"p3", api::field_type_kind::time_point},
        {"p4", api::field_type_kind::time_point}, //TODO with time zone

        {"n0", api::field_type_kind::date},
        {"n1", api::field_type_kind::time_of_day},
        {"n2", api::field_type_kind::time_of_day}, //TODO with time zone
        {"n3", api::field_type_kind::time_point},
        {"n4", api::field_type_kind::time_point}, //TODO with time zone
    };
    {
        auto ps = api::create_parameter_set();
        ps->set_date("p0", date_v{2000, 1, 1});
        ps->set_time_of_day("p1", time_of_day_v{12, 0, 0});
        ps->set_time_of_day("p2", time_of_day_v{12, 0, 0});
        ps->set_time_point("p3", time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}});
        ps->set_time_point("p4", time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}});
        execute_statement( "INSERT INTO TTEMPORALS (K0, K1, K2, K3, K4, C0, C1, C2, C3, C4) VALUES (:p0, :p1, :p2, :p3, :p4, :p0, :p1, :p2, :p3, :p4)", variables, *ps);
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TTEMPORALS", result);
        ASSERT_EQ(1, result.size());
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_date("p0", date_v{2000, 1, 1});
        ps->set_time_of_day("p1", time_of_day_v{12, 0, 0});
        ps->set_time_of_day("p2", time_of_day_v{12, 0, 0});
        ps->set_time_point("p3", time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}});
        ps->set_time_point("p4", time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}});

        ps->set_date("n0", date_v{2000, 2, 2});
        ps->set_time_of_day("n1", time_of_day_v{12, 2, 2});
        ps->set_time_of_day("n2", time_of_day_v{12, 2, 2});
        ps->set_time_point("n3", time_point_v{date_v{2000, 2, 2}, time_of_day_v{12, 2, 2}});
        ps->set_time_point("n4", time_point_v{date_v{2000, 2, 2}, time_of_day_v{12, 2, 2}});
        execute_statement( "UPDATE TTEMPORALS SET C0 = :n0, C1 = :n1, C2 = :n2, C3 = :n3, C4 = :n4 WHERE K0 = :p0 AND K1 = :p1 AND K2 = :p2 AND K3 = :p3 AND K4 = :p4", variables, *ps);

        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TTEMPORALS", result);
        ASSERT_EQ(1, result.size());

        EXPECT_EQ((mock::typed_nullable_record<
            kind::date, kind::time_of_day, kind::time_of_day, kind::time_point, kind::time_point,
            kind::date, kind::time_of_day, kind::time_of_day, kind::time_point, kind::time_point
        >(
            std::tuple{
                dat, tod, tod, tp, tp,
                dat, tod, tod, tp, tp,
            },
            {
                {date_v{2000, 1, 1}}, {time_of_day_v{12, 0, 0}}, {time_of_day_v{12, 0, 0}}, {time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}}}, {time_point_v{date_v{2000, 1, 1}, time_of_day_v{12, 0, 0}}},
                {date_v{2000, 2, 2}}, {time_of_day_v{12, 2, 2}}, {time_of_day_v{12, 2, 2}}, {time_point_v{date_v{2000, 2, 2}, time_of_day_v{12, 2, 2}}}, {time_point_v{date_v{2000, 2, 2}, time_of_day_v{12, 2, 2}}},
            }
        )), result[0]);
    }
}

}
