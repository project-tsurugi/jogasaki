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
#include <future>
#include <sstream>
#include <thread>
#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <tateyama/api/server/mock/request_response.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/constants.h>
#include <jogasaki/datastore/datastore_mock.h>
#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/test_utils/create_file.h>
#include <jogasaki/test_utils/temporary_folder.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/utils/latch.h>
#include <jogasaki/utils/msgbuf_utils.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/utils/tables.h>
#include "jogasaki/proto/sql/common.pb.h"
#include "jogasaki/proto/sql/request.pb.h"
#include "jogasaki/proto/sql/response.pb.h"

#include "../api/api_test_base.h"
#include "service_api_common.h"

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::string_view_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::utils;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::executor::dto;
using namespace jogasaki::scheduler;
using namespace tateyama::api::server;
namespace sql = jogasaki::proto::sql;
using ValueCase = sql::request::Parameter::ValueCase;

using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;
using takatori::datetime::date;
using takatori::datetime::time_of_day;
using takatori::datetime::time_point;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using time_of_day_tz = utils::time_of_day_tz;
using time_point_tz = utils::time_point_tz;
using decimal_v = takatori::decimal::triple;
using ft = meta::field_type_kind;

using jogasaki::api::impl::get_impl;

std::string serialize(sql::request::Request& r);
void deserialize(std::string_view s, sql::response::Response& res);

TEST_F(service_api_test, data_types) {
    execute_statement("create table T1 (C0 int, C1 bigint, C2 double, C3 real, C4 varchar(100), primary key(C0, C1))");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T1(C0, C1, C2, C3, C4) values (:c0, :c1, :c2, :c3, :c4)",
        std::pair{"c0"s, sql::common::AtomType::INT4},
        std::pair{"c1"s, sql::common::AtomType::INT8},
        std::pair{"c2"s, sql::common::AtomType::FLOAT8},
        std::pair{"c3"s, sql::common::AtomType::FLOAT4},
        std::pair{"c4"s, sql::common::AtomType::CHARACTER}
    );
    for(std::size_t i=0; i < 3; ++i) {
        std::vector<parameter> parameters{
            {"c0"s, ValueCase::kInt4Value, std::any{std::in_place_type<std::int32_t>, i}},
            {"c1"s, ValueCase::kInt8Value, std::any{std::in_place_type<std::int64_t>, i}},
            {"c2"s, ValueCase::kFloat8Value, std::any{std::in_place_type<double>, i}},
            {"c3"s, ValueCase::kFloat4Value, std::any{std::in_place_type<float>, i}},
            {"c4"s, ValueCase::kCharacterValue, std::any{std::in_place_type<std::string>, std::to_string(i)}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, stats] = decode_execute_result(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, C1, C2, C3, C4 from T1 where C1 > :c1 and C2 > :c2 and C4 > :c4 order by C0",
        std::pair{"c1"s, sql::common::AtomType::INT8},
        std::pair{"c2"s, sql::common::AtomType::FLOAT8},
        std::pair{"c4"s, sql::common::AtomType::CHARACTER}
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{
            {"c1"s, ValueCase::kInt8Value, std::any{std::in_place_type<std::int64_t>, 0}},
            {"c2"s, ValueCase::kFloat8Value, std::any{std::in_place_type<double>, 0.0}},
            {"c4"s, ValueCase::kCharacterValue, std::any{std::in_place_type<std::string>, "0"}},
        };
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"C0", common_column::atom_type::int4},  // nullable is not sent now
                {"C1", common_column::atom_type::int8},  // nullable is not sent now
                {"C2", common_column::atom_type::float8},  // nullable is not sent now
                {"C3", common_column::atom_type::float4},  // nullable is not sent now
                {"C4", common_column::atom_type::character},  // nullable is not sent now
            };
            exp[4].varying_ = true;
            ASSERT_EQ(exp, cols);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(2, v.size());
                auto exp1 = mock::create_nullable_record<meta::field_type_kind::int4, meta::field_type_kind::int8, meta::field_type_kind::float8, meta::field_type_kind::float4, meta::field_type_kind::character>(1, 1, 1.0, 1.0, accessor::text{"1"sv});
                auto exp2 = mock::create_nullable_record<meta::field_type_kind::int4, meta::field_type_kind::int8, meta::field_type_kind::float8, meta::field_type_kind::float4, meta::field_type_kind::character>(2, 2, 2.0, 2.0, accessor::text{"2"sv});
                EXPECT_EQ(exp1, v[0]);
                EXPECT_EQ(exp2, v[1]);
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, char_varchar) {
    // verify result set metadata for char and varchar columns - both should be returned as varchar(*)
    execute_statement("create table t (c0 char(10), c1 varchar(10))");
    execute_statement("insert into t values ('1234567890', '1234567890')");

    api::transaction_handle tx_handle{};

    test_begin(tx_handle);

    auto varchar_aster = meta::field_type{std::make_shared<meta::character_field_option>()};

    std::vector<common_column> exp{
        common_column{"c0", common_column::atom_type::character},  // nullable is not sent now
        common_column{"c1", common_column::atom_type::character},  // nullable is not sent now
    };
    exp[0].varying_ = false;
    exp[1].varying_ = true;
    test_query(
        "select c0, c1 from t",
        tx_handle,
        exp,
        {
            true,
            true
        },
        {
                mock::typed_nullable_record<ft::character, ft::character>(
                std::tuple{
                    varchar_aster, varchar_aster
                },
                { accessor::text{"1234567890"}, accessor::text{"1234567890"},
                }
            )
        },
        {"c0", "c1"}
    );
    test_commit(tx_handle);
}

TEST_F(service_api_test, decimals) {
    execute_statement("create table TDECIMALS (K0 decimal(3,0), K1 decimal(5,3), K2 decimal(10,1), C0 decimal(3,0), C1 decimal(5,3), C2 decimal(10,1), primary key(K0, K1, K2))");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into TDECIMALS(K0, K1, K2, C0, C1, C2) values (:p0, :p1, :p2, :p3, :p4, :p5)",
        std::pair{"p0"s, sql::common::AtomType::DECIMAL},
        std::pair{"p1"s, sql::common::AtomType::DECIMAL},
        std::pair{"p2"s, sql::common::AtomType::DECIMAL},
        std::pair{"p3"s, sql::common::AtomType::DECIMAL},
        std::pair{"p4"s, sql::common::AtomType::DECIMAL},
        std::pair{"p5"s, sql::common::AtomType::DECIMAL}
    );

    auto v111 = decimal_v{1, 0, 111, 0}; // 111
    auto v11_111 = decimal_v{1, 0, 11111, -3}; // 11.111
    auto v11111_1 = decimal_v{1, 0, 111111, -1}; // 11111.1
    auto v222 = decimal_v{1, 0, 222, 0}; // 222
    auto v22_222 = decimal_v{1, 0, 22222, -3}; // 22.222
    auto v22222_2 = decimal_v{1, 0, 222222, -1}; // 22222.2
    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kDecimalValue, std::any{std::in_place_type<takatori::decimal::triple>, v111}},
            {"p1"s, ValueCase::kDecimalValue, std::any{std::in_place_type<takatori::decimal::triple>, v11_111}},
            {"p2"s, ValueCase::kDecimalValue, std::any{std::in_place_type<takatori::decimal::triple>, v11111_1}},
            {"p3"s, ValueCase::kDecimalValue, std::any{std::in_place_type<takatori::decimal::triple>, v222}},
            {"p4"s, ValueCase::kDecimalValue, std::any{std::in_place_type<takatori::decimal::triple>, v22_222}},
            {"p5"s, ValueCase::kDecimalValue, std::any{std::in_place_type<takatori::decimal::triple>, v22222_2}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, stats] = decode_execute_result(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select * from TDECIMALS"
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"K0", common_column::atom_type::decimal},  // nullable is not sent now // no ps
                {"K1", common_column::atom_type::decimal},  // nullable is not sent now // no ps
                {"K2", common_column::atom_type::decimal},  // nullable is not sent now // no ps
                {"C0", common_column::atom_type::decimal},  // nullable is not sent now // no ps
                {"C1", common_column::atom_type::decimal},  // nullable is not sent now // no ps
                {"C2", common_column::atom_type::decimal},  // nullable is not sent now // no ps
            };
            ASSERT_EQ(exp, cols);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());

                // currently result type of decimal has no precision/scale info.
//                auto dec_3_0 = meta::field_type{std::make_shared<meta::decimal_field_option>(3, 0)};
//                auto dec_5_3 = meta::field_type{std::make_shared<meta::decimal_field_option>(5, 3)};
//                auto dec_10_1 = meta::field_type{std::make_shared<meta::decimal_field_option>(10, 1)};
                auto dec_3_0 = meta::field_type{std::make_shared<meta::decimal_field_option>()};
                auto dec_5_3 = meta::field_type{std::make_shared<meta::decimal_field_option>()};
                auto dec_10_1 = meta::field_type{std::make_shared<meta::decimal_field_option>()};
                EXPECT_EQ((mock::typed_nullable_record<
                    ft::decimal, ft::decimal, ft::decimal,
                    ft::decimal, ft::decimal, ft::decimal
                >(
                    std::tuple{
                        dec_3_0, dec_5_3, dec_10_1,
                        dec_3_0, dec_5_3, dec_10_1,
                    },
                    {
                        v111, v11_111, v11111_1,
                        v222, v22_222, v22222_2,
                    }
                )), v[0]);
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, temporal_types) {
    execute_statement("create table TTEMPORALS (K0 date, K1 time, K2 time with time zone, K3 timestamp, K4 timestamp with time zone, C0 date, C1 time, C2 time with time zone, C3 timestamp, C4 timestamp with time zone, primary key(K0, K1, K2, K3, K4))");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into TTEMPORALS(K0, K1, K2, K3, K4, C0, C1, C2, C3, C4) values (:p0, :p1, :p2, :p3, :p4, :p0, :p1, :p2, :p3, :p4)",
        std::pair{"p0"s, sql::common::AtomType::DATE},
        std::pair{"p1"s, sql::common::AtomType::TIME_OF_DAY},
        std::pair{"p2"s, sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE},
        std::pair{"p3"s, sql::common::AtomType::TIME_POINT},
        std::pair{"p4"s, sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE}
    );

    auto d2000_1_1 = date_v{2000, 1, 1};
    auto t12_0_0 = time_of_day_v{12, 0, 0};
    auto t3_0_0 = time_of_day_v{3, 0, 0};
    auto tp2000_1_1_12_0_0 = time_point_v{d2000_1_1, t12_0_0};
    auto tp2000_1_1_3_0_0 = time_point_v{d2000_1_1, t3_0_0};

    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kDateValue, std::any{std::in_place_type<date_v>, d2000_1_1}},
            {"p1"s, ValueCase::kTimeOfDayValue, std::any{std::in_place_type<time_of_day_v>, t12_0_0}},
            {"p2"s, ValueCase::kTimeOfDayWithTimeZoneValue, std::any{std::in_place_type<time_of_day_tz>, time_of_day_tz{t12_0_0, 9*60}}},
            {"p3"s, ValueCase::kTimePointValue, std::any{std::in_place_type<time_point_v>, tp2000_1_1_12_0_0}},
            {"p4"s, ValueCase::kTimePointWithTimeZoneValue, std::any{std::in_place_type<time_point_tz>, time_point_tz{tp2000_1_1_12_0_0, 9*60}}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, stats] = decode_execute_result(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select * from TTEMPORALS"
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"K0", common_column::atom_type::date},  // nullable is not sent now
                {"K1", common_column::atom_type::time_of_day},  // nullable is not sent now
                {"K2", common_column::atom_type::time_of_day_with_time_zone},  // nullable is not sent now
                {"K3", common_column::atom_type::time_point},  // nullable is not sent now
                {"K4", common_column::atom_type::time_point_with_time_zone},  // nullable is not sent now
                {"C0", common_column::atom_type::date},  // nullable is not sent now
                {"C1", common_column::atom_type::time_of_day},  // nullable is not sent now
                {"C2", common_column::atom_type::time_of_day_with_time_zone},  // nullable is not sent now
                {"C3", common_column::atom_type::time_point},  // nullable is not sent now
                {"C4", common_column::atom_type::time_point_with_time_zone},  // nullable is not sent now
            };
            ASSERT_EQ(exp, cols);
            ASSERT_EQ(10, cols.size());
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());

                auto dat = meta::field_type{meta::field_enum_tag<ft::date>};
                auto tod = meta::field_type{std::make_shared<meta::time_of_day_field_option>(false)};
                auto tp = meta::field_type{std::make_shared<meta::time_point_field_option>(false)};
                auto todtz = meta::field_type{std::make_shared<meta::time_of_day_field_option>(true)};
                auto tptz = meta::field_type{std::make_shared<meta::time_point_field_option>(true)};
                EXPECT_EQ((mock::typed_nullable_record<
                    ft::date, ft::time_of_day, ft::time_of_day, ft::time_point, ft::time_point,
                    ft::date, ft::time_of_day, ft::time_of_day, ft::time_point, ft::time_point
                >(
                    std::tuple{
                        dat, tod, todtz, tp, tptz,
                        dat, tod, todtz, tp, tptz,
                    },
                    {
                        d2000_1_1, t12_0_0, t3_0_0, tp2000_1_1_12_0_0, tp2000_1_1_3_0_0,
                        d2000_1_1, t12_0_0, t3_0_0, tp2000_1_1_12_0_0, tp2000_1_1_3_0_0,
                    }
                )), v[0]);
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, timestamptz) {
    // there was an issue with timestamp close to 0000-00-00
    execute_statement("create table T (C0 TIMESTAMP WITH TIME ZONE)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T values (:p0)",
        std::pair{"p0"s, sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE}
    );

    auto tod = time_of_day{0, 2, 48, 91383000ns};
    auto tp = time_point{date{1, 1, 1}, tod};

    auto exp_tp = time_point{date{0, 12, 31}, time_of_day{15, 2, 48, 91383000ns}};

    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kTimePointWithTimeZoneValue, std::any{std::in_place_type<time_point_tz>, time_point_tz{tp, 9*60}}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, stats] = decode_execute_result(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select * from T"
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                    {"C0", common_column::atom_type::time_point_with_time_zone},  // nullable is not sent now
                };
            ASSERT_EQ(exp, cols);
            ASSERT_EQ(1, cols.size());
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());

                auto tptz = meta::field_type{std::make_shared<meta::time_point_field_option>(true)};
                EXPECT_EQ((mock::typed_nullable_record<ft::time_point>(
                    std::tuple{tptz}, {exp_tp}
                )), v[0]);
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, timestamptz_with_offset) {
    // offset conversion for timestamptz is done in service.cpp
    global::config_pool()->zone_offset(9*60);

    // there was an issue with timestamp close to 0000-00-00
    execute_statement("create table T (C0 TIMESTAMP WITH TIME ZONE)");
    execute_statement("insert into T values (TIMESTAMP WITH TIME ZONE'2000-01-01 00:00:00+09:00')");
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select * from T"
    );
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"C0", common_column::atom_type::time_point_with_time_zone},  // nullable is not sent now
            };
            ASSERT_EQ(exp, cols);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());

                // currently deserialize_msg discard the offset part because record_ref field has no room to store it
                EXPECT_EQ(
                    (mock::typed_nullable_record<ft::time_point>(
                        std::tuple{meta::time_point_type(true)},
                        {time_point{date{2000, 1, 1}, time_of_day{0, 0, 0}}}
                    )),
                    v[0]
                );
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, binary_type) {
    execute_statement("create table T (C0 VARBINARY(5), C1 BINARY(5))");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T(C0, C1) values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::OCTET},
        std::pair{"p1"s, sql::common::AtomType::OCTET}
    );

    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kOctetValue, std::any{std::in_place_type<std::string>, "\x01\x02\x03"s}},
            {"p1"s, ValueCase::kOctetValue, std::any{std::in_place_type<std::string>, "\x04\x05\x06"s}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, stats] = decode_execute_result(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, C1 from T"
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"C0", common_column::atom_type::octet},  // nullable is not sent now
                {"C1", common_column::atom_type::octet},  // nullable is not sent now
            };
            exp[0].varying_ = true;
            exp[1].varying_ = false;
            ASSERT_EQ(exp, cols);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());

                EXPECT_EQ((mock::typed_nullable_record<ft::octet, ft::octet>(
                    std::tuple{meta::octet_type(true), meta::octet_type(true)}, // currently service.cpp layer does not handle varying=true/false and all octet columns are varying
                    {accessor::binary{"\x01\x02\x03"}, accessor::binary{"\x04\x05\x06\x00\x00"}}
                )), v[0]);
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, long_binary_data) {
    execute_statement("create table T (C0 BIGINT, C1 VARBINARY(*))");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T(C0, C1) values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::INT8},
        std::pair{"p1"s, sql::common::AtomType::OCTET}
    );

    std::string long_str(octet_type_max_length_for_value, '\x01');
    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kInt8Value, std::any{std::in_place_type<std::int64_t>, 0}},
            {"p1"s, ValueCase::kOctetValue, std::any{std::in_place_type<std::string>, long_str}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, stats] = decode_execute_result(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C1 from T"
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"C1", common_column::atom_type::octet},  // nullable is not sent now
            };
            exp[0].varying_ = true;
            ASSERT_EQ(exp, cols);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());

                EXPECT_EQ((mock::typed_nullable_record<ft::octet>(
                    std::tuple{meta::octet_type(true)}, // currently service.cpp layer does not handle varying=true/false and all octet columns are varying
                    {accessor::binary{long_str}}
                )), v[0]);
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, boolean_type) {
    db_impl()->configuration()->support_boolean(true);
    execute_statement("create table T (C0 BOOLEAN PRIMARY KEY, C1 BOOLEAN)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::BOOLEAN},
        std::pair{"p1"s, sql::common::AtomType::BOOLEAN}
    );

    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kBooleanValue, std::any{std::in_place_type<std::int8_t>, 0}},
            {"p1"s, ValueCase::kBooleanValue, std::any{std::in_place_type<std::int8_t>, 1}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, stats] = decode_execute_result(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, C1 from T"
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"C0", common_column::atom_type::boolean},  // nullable is not sent now
                {"C1", common_column::atom_type::boolean},  // nullable is not sent now
            };
            ASSERT_EQ(exp, cols);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());

                EXPECT_EQ((mock::typed_nullable_record<ft::boolean, ft::boolean>(
                    std::tuple{meta::boolean_type(), meta::boolean_type()},
                    {static_cast<std::int8_t>(0), static_cast<std::int8_t>(1)}
                )), v[0]);
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

}  // namespace jogasaki::api
