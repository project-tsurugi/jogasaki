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

TEST_F(service_api_test, explain_insert) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0(C0, C1) values (:c0, :c1)",
        std::pair{"c0"s, sql::common::AtomType::INT8},
        std::pair{"c1"s, sql::common::AtomType::FLOAT8}
    );
    {
        std::vector<parameter> parameters{
            {"c0"s, ValueCase::kInt8Value, std::any{std::in_place_type<std::int64_t>, 1}},
            {"c1"s, ValueCase::kFloat8Value, std::any{std::in_place_type<double>, 10.0}},
        };
        auto s = encode_explain(stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [result, id, version, cols, error] = decode_explain(res->body_);
        ASSERT_FALSE(result.empty());
        EXPECT_EQ(sql_proto_explain_format_id, id);
        EXPECT_EQ(sql_proto_explain_format_version, version);
        ASSERT_TRUE(cols.empty());

        LOG(INFO) << result;
    }
}

TEST_F(service_api_test, explain_query) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::uint64_t stmt_handle{};

    test_prepare(
        stmt_handle,
        "select C0, C1 from T0 where C0 = :c0 and C1 = :c1",
        std::pair{"c0"s, sql::common::AtomType::INT8},
        std::pair{"c1"s, sql::common::AtomType::FLOAT8}
    );
    {
        std::vector<parameter> parameters{
            {"c0"s, ValueCase::kInt8Value, std::any{std::in_place_type<std::int64_t>, 1}},
            {"c1"s, ValueCase::kFloat8Value, std::any{std::in_place_type<double>, 10.0}},
        };
        auto s = encode_explain(stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [result, id, version, cols, error] = decode_explain(res->body_);
        ASSERT_FALSE(result.empty());
        EXPECT_EQ(sql_proto_explain_format_id, id);
        EXPECT_EQ(sql_proto_explain_format_version, version);
        std::vector<common_column> exp{
            {"C0", common_column::atom_type::int8},  // nullable is not sent now
            {"C1", common_column::atom_type::float8},  // nullable is not sent now
        };
        ASSERT_EQ(exp, cols);
        LOG(INFO) << result;
    }
}

TEST_F(service_api_test, explain_error_invalid_handle) {
    // verify error when handle is invalid (zero)
    std::uint64_t stmt_handle{};
    {
        auto s = encode_explain(stmt_handle, {});
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [result, id, version, cols, error] = decode_explain(res->body_);
        ASSERT_TRUE(result.empty());
        ASSERT_TRUE(cols.empty());

        ASSERT_EQ(error_code::sql_execution_exception, error.code_);
        ASSERT_FALSE(error.message_.empty());
        LOG(INFO) << error.message_;
    }
}

TEST_F(service_api_test, explain_error_invalid_handle_non_zero_handle) {
    // same as explain_error_invalid_handle, but with non-zero handle
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "select C0, C1 from T0 where C0 = :c0 and C1 = :c1",
        std::pair{"c0"s, sql::common::AtomType::INT8},
        std::pair{"c1"s, sql::common::AtomType::FLOAT8}
    );
    test_dispose_prepare(stmt_handle);
    {
        auto s = encode_explain(stmt_handle, {});
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [result, id, version, cols, error] = decode_explain(res->body_);
        ASSERT_TRUE(result.empty());
        ASSERT_TRUE(cols.empty());

        ASSERT_EQ(error_code::statement_not_found_exception, error.code_);
        ASSERT_FALSE(error.message_.empty());
        LOG(INFO) << error.message_;
    }
}

TEST_F(service_api_test, explain_error_missing_parameter) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::uint64_t stmt_handle{};

    test_prepare(
        stmt_handle,
        "select C0, C1 from T0 where C0 = :c0 and C1 = :c1",
        std::pair{"c0"s, sql::common::AtomType::INT8},
        std::pair{"c1"s, sql::common::AtomType::FLOAT8}
    );
    {
        auto s = encode_explain(stmt_handle, {});
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [explained, id, version, cols, error] = decode_explain(res->body_);
        ASSERT_TRUE(explained.empty());
        ASSERT_TRUE(cols.empty());
        ASSERT_EQ(error_code::unresolved_placeholder_exception, error.code_);
        ASSERT_FALSE(error.message_.empty());
    }
}

TEST_F(service_api_test, explain_unauthorized) {
    // verify the error code correctly returned
    execute_statement("create table t (c0 bigint primary key)");
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "select * from t"
    );
    std::vector<parameter> parameters{};
    auto s = encode_explain(stmt_handle, parameters);
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


TEST_F(service_api_test, explain_by_text) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    auto s = encode_explain_by_text("select C0, C1 from T0 where C0 = 1 and C1 = 1.0");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, id, version, cols, error] = decode_explain(res->body_);
    ASSERT_FALSE(result.empty());
    EXPECT_EQ(sql_proto_explain_format_id, id);
    EXPECT_EQ(sql_proto_explain_format_version, version);
    std::vector<common_column> exp{
        {"C0", common_column::atom_type::int8},  // nullable is not sent now
        {"C1", common_column::atom_type::float8},  // nullable is not sent now
    };
    ASSERT_EQ(exp, cols);
    LOG(INFO) << result;
}

TEST_F(service_api_test, explain_by_text_error_on_prepare) {
    auto s = encode_explain_by_text("select * from dummy_table");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, id, version, cols, error] = decode_explain(res->body_);
    ASSERT_TRUE(result.empty());
    ASSERT_TRUE(cols.empty());
    ASSERT_EQ(error_code::symbol_analyze_exception, error.code_);
    ASSERT_FALSE(error.message_.empty());
    LOG(INFO) << result;
}

TEST_F(service_api_test, explain_by_text_bypass_restriction) {
    // verify explain by text does not return on restricted features
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    auto s = encode_explain_by_text("select * from T0 union all select * from T0");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, id, version, cols, error] = decode_explain(res->body_);
    ASSERT_FALSE(result.empty());
    LOG(INFO) << result;
}

TEST_F(service_api_test, explain_by_text_unauthorized) {
    // verify the error code correctly returned
    execute_statement("create table t (c0 bigint primary key)");
    std::vector<parameter> parameters{};
    auto s = encode_explain_by_text("select * from t");
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

}  // namespace jogasaki::api
