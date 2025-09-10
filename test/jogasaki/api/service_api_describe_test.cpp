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

#include "api_test_base.h"
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

using atom_type = dto::common_column::atom_type;

TEST_F(service_api_test, describe_table) {
    execute_statement("create table t (c0 bigint primary key, c1 double)");
    auto s = encode_describe_table("t");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, error] = decode_describe_table(res->body_);
    dto::describe_table exp{
        "t",
        {
            {"c0", atom_type::int8, false},
            {"c1", atom_type::float8, true},
        },
        {"c0"},
    };
    EXPECT_EQ(exp, result);
}

TEST_F(service_api_test, describe_table_not_found) {
    auto s = encode_describe_table("DUMMY");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, error] = decode_describe_table(res->body_);
    ASSERT_EQ(error_code::target_not_found_exception, error.code_);
    LOG(INFO) << "error: " << error.message_;
}

TEST_F(service_api_test, describe_table_unauthorized) {
    // verify the error code correctly returned
    execute_statement("create table t (c0 bigint primary key)");
    auto s = encode_describe_table("t");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    req->session_info_.user_type_ = user_type::standard;
    req->session_info_.username_ = "user1";
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_EQ(tateyama::proto::diagnostics::Code::PERMISSION_ERROR, res->error_.code());
}

TEST_F(service_api_test, describe_table_length_ps) {
    execute_statement("create table t (c0 varchar(*) primary key, c1 char(10), c2 decimal(5,3), c3 decimal(*,3))");
    auto s = encode_describe_table("t");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, error] = decode_describe_table(res->body_);

    dto::describe_table exp{
        "t",
        {
            {"c0", atom_type::character, false, true},
            {"c1", atom_type::character, true, 10u},
            {"c2", atom_type::decimal, true, std::nullopt, 5u, 3u},
            {"c3", atom_type::decimal, true, std::nullopt, 38u, 3u},
        },
        {"c0"},
    };
    exp.columns_[0].varying_ = true;
    exp.columns_[1].varying_ = false;
    EXPECT_EQ(exp, result);
}

TEST_F(service_api_test, describe_table_temporal_types) {
    // verify with_offset is correctly reflected on the output schema
    execute_statement("create table t (c0 DATE, c1 TIME, c2 TIMESTAMP, c3 TIME WITH TIME ZONE, c4 TIMESTAMP WITH TIME ZONE)");
    auto s = encode_describe_table("t");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, error] = decode_describe_table(res->body_);
    dto::describe_table exp{
        "t",
        {
            {"c0", atom_type::date, true},
            {"c1", atom_type::time_of_day, true},
            {"c2", atom_type::time_point, true},
            {"c3", atom_type::time_of_day_with_time_zone, true},
            {"c4", atom_type::time_point_with_time_zone, true},
        },
        {},
    };
    EXPECT_EQ(exp, result);
}

}  // namespace jogasaki::api
