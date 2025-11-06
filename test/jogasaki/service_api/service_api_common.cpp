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
#include "service_api_common.h"

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

void service_api_test::test_begin(
    api::transaction_handle& tx_handle,
    bool readonly,
    bool is_long,
    std::vector<std::string> const& write_preserves,
    std::string_view label,
    bool modifies_definitions
) {
    begin_result result{};
    test_begin(result, readonly, is_long, write_preserves, label, modifies_definitions);
    tx_handle = {result.handle_.surrogate_id(), session_id_};
}

void service_api_test::test_begin(
    begin_result& result,
    bool readonly,
    bool is_long,
    std::vector<std::string> const& write_preserves,
    std::string_view label,
    bool modifies_definitions
) {
    auto s = encode_begin(readonly, is_long, write_preserves, label, modifies_definitions);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);
    result = decode_begin(res->body_);
}

void service_api_test::test_commit(
    api::transaction_handle& tx_handle,
    bool auto_dispose_on_commit_success,
    error_code expected
) {
    auto s = encode_commit(tx_handle, auto_dispose_on_commit_success);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);

    {
        auto [success, error] = decode_result_only(res->body_);
        if(expected == error_code::none) {
            ASSERT_TRUE(success);
        } else {
            ASSERT_FALSE(success);
            ASSERT_EQ(expected, error.code_);
        }
    }
}

void service_api_test::test_rollback(api::transaction_handle& tx_handle) {
    auto s = encode_rollback(tx_handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);
    auto [success, error] = decode_result_only(res->body_);
    ASSERT_TRUE(success);
}

void service_api_test::test_dispose_prepare(std::uint64_t handle) {
    auto s = encode_dispose_prepare(handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    auto [success, error] = decode_result_only(res->body_);
    ASSERT_TRUE(success);
}

void service_api_test::test_statement(
    std::string_view sql, api::transaction_handle tx_handle, error_code exp) {
    std::shared_ptr<request_statistics> stats{};
    test_statement(sql, tx_handle, exp, stats);
}

void service_api_test::test_statement(
    std::string_view sql, api::transaction_handle tx_handle, error_code exp, std::shared_ptr<request_statistics>& stats) {
    auto s = encode_execute_statement(tx_handle, sql);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_TRUE(res->all_released());

    auto [success, error, statistics] = decode_execute_result(res->body_);
    stats = std::move(statistics);
    if(exp == error_code::none) {
        ASSERT_TRUE(success);
    } else {
        ASSERT_FALSE(success);
        ASSERT_EQ(exp, error.code_);
    }
}
void service_api_test::test_statement(std::string_view sql, api::transaction_handle tx_handle) {
    test_statement(sql, tx_handle, error_code::none);
}

void service_api_test::test_statement(std::string_view sql, api::transaction_handle tx_handle, std::shared_ptr<request_statistics>& stats) {
    test_statement(sql, tx_handle, error_code::none, stats);
}

void service_api_test::test_statement(std::string_view sql) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_statement(sql, tx_handle);
    test_commit(tx_handle);
}

void service_api_test::test_statement(std::string_view sql, std::shared_ptr<request_statistics>& stats) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_statement(sql, tx_handle, stats);
    test_commit(tx_handle);
}

void service_api_test::test_query(
    std::string_view sql,
    api::transaction_handle tx_handle,
    std::vector<dto::common_column> const& column_types,
    std::vector<bool> const& nullabilities,
    std::vector<mock::basic_record> const& expected,
    std::vector<std::string> const& exp_colnames,
    bool sort_before_compare
) {
    auto s = encode_execute_query(tx_handle, sql);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_TRUE(res->all_released());

    {
        auto [name, cols] = decode_execute_query(res->body_head_);
        ASSERT_EQ(column_types, cols);
        {
            ASSERT_TRUE(res->channel_);
            auto& ch = *res->channel_;
            auto m = create_record_meta(cols);
            auto v = deserialize_msg(ch.view(), m);
            ASSERT_EQ(expected.size(), v.size());
            if (sort_before_compare) {
                std::sort(v.begin(), v.end());
            }
            for(std::size_t i=0, n=v.size(); i<n; ++i) {
                EXPECT_EQ(expected[i], v[i]);
            }
            EXPECT_TRUE(ch.all_released()) << "# of writers:" << ch.buffers_.size() << " released:" << ch.released_;
        }
    }
    {
        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
}

void service_api_test::test_query(std::string_view query) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_query(
        query,
        tx_handle,
        {
            {"C0", common_column::atom_type::int8},    // nullable is not sent now
            {"C1", common_column::atom_type::float8},   // nullable is not sent now
        },
        {
            true,
            true
        },
        {mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0)},
        {"C0", "C1"}
    );
    test_commit(tx_handle);
}

void service_api_test::test_prepared_statement(std::uint64_t stmt_handle, api::transaction_handle tx_handle, error_code exp) {
    std::shared_ptr<request_statistics> stats{};
    return test_prepared_statement(stmt_handle, tx_handle, exp, stats);
}

void service_api_test::test_prepared_statement(
    std::uint64_t stmt_handle,
    api::transaction_handle tx_handle,
    error_code exp,
    std::shared_ptr<request_statistics>& stats) {

    {
        std::vector<parameter> parameters{
                {"p0"s, ValueCase::kInt8Value, std::any{std::in_place_type<std::int64_t>, 1}},
                {"p1"s, ValueCase::kFloat8Value, std::any{std::in_place_type<double>, 10.0}},
            };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, statistics] = decode_execute_result(res->body_);

        if(exp == error_code::none) {
            ASSERT_TRUE(success);
            stats = std::move(statistics);
        } else {
            ASSERT_FALSE(success);
            ASSERT_EQ(exp, error.code_);
        }
    }
}

void service_api_test::test_get_lob(std::uint64_t id, std::string_view expected_path) {
    auto s = encode_get_large_object_data(id);

    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [channel_name, contents, error] = decode_get_large_object_data(res->body_);
    bool found = false;
    for (auto&& b : res->blobs_) {
        if (b->channel_name() == channel_name) {
            ASSERT_EQ(expected_path, b->path());
            found = true;
        }
    }
    ASSERT_TRUE(found);
}

void service_api_test::test_get_tx_status(api::transaction_handle tx_handle, std::optional<::jogasaki::proto::sql::response::TransactionStatus> expected_status, error_code expected_err) {
    auto s = encode_get_transaction_status(tx_handle);

    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [status, msg, error] = decode_get_transaction_status(res->body_);
    if (expected_status.has_value()) {
        ASSERT_EQ(expected_status.value(), status);
    } else {
        ASSERT_EQ(expected_err, error.code_);
    }
    if (! msg.empty()) {
        std::cerr << "status: " << status <<  " msg:" << msg << std::endl;
    }
}

void service_api_test::execute_statement_as_query(std::string_view sql) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    auto s = encode_execute_query(tx_handle, "insert into T0(C0, C1) values (1, 10.0)");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(error_code::inconsistent_statement_exception, error.code_);
    ASSERT_FALSE(error.message_.empty());
    test_commit(tx_handle);
}

void service_api_test::test_dump(std::vector<std::string>& files, std::string_view dir, error_code expected) {
    std::string p{dir.empty() ? service_api_test::temporary_.path() : std::string{dir}};
    test_statement("insert into T0(C0, C1) values (0, 0.0)");
    test_statement("insert into T0(C0, C1) values (1, 10.0)");
    test_statement("insert into T0(C0, C1) values (2, 20.0)");
    test_statement("insert into T0(C0, C1) values (3, 30.0)");
    test_statement("insert into T0(C0, C1) values (4, 40.0)");
    test_statement("insert into T0(C0, C1) values (5, 50.0)");
    test_statement("insert into T0(C0, C1) values (6, 60.0)");
    test_statement("insert into T0(C0, C1) values (7, 70.0)");
    test_statement("insert into T0(C0, C1) values (8, 80.0)");
    test_statement("insert into T0(C0, C1) values (9, 90.0)");
    test_statement("insert into T0(C0, C1) values (10, 100.0)");
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, C1 from T0 where C0 > :c0 and C1 > :c1",
        std::pair{"c0"s, sql::common::AtomType::INT8},
        std::pair{"c1"s, sql::common::AtomType::FLOAT8}
    );
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    do {
        std::vector<parameter> parameters{
            {"c0"s, ValueCase::kInt8Value, std::any{std::in_place_type<std::int64_t>, 0}},
            {"c1"s, ValueCase::kFloat8Value, std::any{std::in_place_type<double>, 0.0}},
        };
        auto s = encode_execute_dump(tx_handle, query_handle, parameters, p);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->all_released());
        ASSERT_TRUE(st);
        if (expected != error_code::none) {
            break;
        }
        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"file_name", common_column::atom_type::character, std::nullopt, true},  // nullable is not sent now
            };
            exp[0].varying_ = true;
            ASSERT_EQ(exp, cols);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());
                LOG(INFO) << v[0];
                files.emplace_back(static_cast<std::string>(v[0].get_value<accessor::text>(0)));
                EXPECT_TRUE(ch.all_released());
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_TRUE(success);
        }
    } while(0);
    test_commit(tx_handle);
    test_dispose_prepare(query_handle);
}


using atom_type = dto::common_column::atom_type;

void service_api_test::test_dispose_transaction(
    api::transaction_handle tx_handle,
    error_code expected
) {
    auto s = encode_dispose_transaction(tx_handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [success, error] = decode_result_only(res->body_);
    if(expected == error_code::none) {
        ASSERT_TRUE(success);
    } else {
        ASSERT_FALSE(success);
        ASSERT_EQ(expected, error.code_);
    }
}

void service_api_test::test_cancel_transaction_commit(api::transaction_handle tx_handle, bool auto_dispose_on_commit_success) {
    auto s = encode_commit(tx_handle, auto_dispose_on_commit_success);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    res->cancel();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_TRUE(res->all_released());

    auto& rec = res->error_;
    EXPECT_EQ(::tateyama::proto::diagnostics::Code::OPERATION_CANCELED, rec.code());
}

void service_api_test::test_cancel_transaction_begin(api::transaction_handle tx_handle, std::string_view label) {
    auto s = encode_begin(false, true, {}, label, false);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    res->cancel();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_TRUE(res->all_released());

    auto& rec = res->error_;
    EXPECT_EQ(::tateyama::proto::diagnostics::Code::OPERATION_CANCELED, rec.code());
}

void service_api_test::test_cancel_statement(
    std::string_view sql, api::transaction_handle tx_handle) {
    auto s = encode_execute_statement(tx_handle, sql);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    res->cancel();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_TRUE(res->all_released());

    auto& rec = res->error_;
    EXPECT_EQ(::tateyama::proto::diagnostics::Code::OPERATION_CANCELED, rec.code());
}

void enable_request_cancel(request_cancel_kind kind) {
    auto cfg = global::config_pool();
    auto c = std::make_shared<request_cancel_config>();
    c->enable(kind);
    cfg->req_cancel_config(std::move(c));
}

}  // namespace jogasaki::api
