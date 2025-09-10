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

TEST_F(service_api_test, begin_and_commit) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_commit(tx_handle);
}

TEST_F(service_api_test, error_on_commit) {
    api::transaction_handle tx_handle{};
    auto s = encode_commit(tx_handle, true);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(error_code::sql_execution_exception, error.code_);
    ASSERT_FALSE(error.message_.empty());
}

TEST_F(service_api_test, rollback) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    {
        auto s = encode_rollback(tx_handle);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
}

TEST_F(service_api_test, error_on_rollback) {
    api::transaction_handle tx_handle{};
    auto s = encode_rollback(tx_handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(error_code::sql_execution_exception, error.code_);
    ASSERT_FALSE(error.message_.empty());
}

TEST_F(service_api_test, prepare_and_dispose) {
    execute_statement("create table t (c0 int primary key)");
    std::uint64_t handle{};
    test_prepare(handle, "select * from t");
    test_dispose_prepare(handle);
}

TEST_F(service_api_test, error_prepare) {
    utils_raise_exception_on_error = false;
    {
        std::uint64_t handle{};
        test_error_prepare(handle, "select * from DUMMY");
    }
    {
        std::uint64_t handle{};
        test_error_prepare(handle, "bad sql statement");
    }
}

TEST_F(service_api_test, error_prepare_with_unsupported_parameter_type) {
    execute_statement("create table t (c0 varchar(10))");
    utils_raise_exception_on_error = false;
    {
        std::uint64_t handle{};
        test_error_prepare(
            handle,
            "insert into t values (:p0)",
            std::pair{"p0"s, sql::common::AtomType::CLOB}
        );
    }
}

TEST_F(service_api_test, error_on_dispose) {
    std::uint64_t handle{0};
    auto s = encode_dispose_prepare(handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);

    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(error_code::sql_execution_exception, error.code_);
    ASSERT_FALSE(error.message_.empty());
}

TEST_F(service_api_test, execute_statement_and_query) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    test_statement("insert into T0(C0, C1) values (1, 10.0)");
    test_query();
}

TEST_F(service_api_test, execute_prepared_statement_and_query) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
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
        "select C0, C1 from T0 where C0 = :c0 and C1 = :c1",
        std::pair{"c0"s, sql::common::AtomType::INT8},
        std::pair{"c1"s, sql::common::AtomType::FLOAT8}
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{
            {"c0"s, ValueCase::kInt8Value, std::any{std::in_place_type<std::int64_t>, 1}},
            {"c1"s, ValueCase::kFloat8Value, std::any{std::in_place_type<double>, 10.0}},
        };
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->all_released());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"C0", common_column::atom_type::int8},  // nullable is not sent now
                {"C1", common_column::atom_type::float8},  // nullable is not sent now
            };
            ASSERT_EQ(exp, cols);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());
                EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0)), v[0]);
                EXPECT_TRUE(ch.all_released());
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

TEST_F(service_api_test, execute_statement_and_query_multi_thread) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory causes problem accessing from multiple threads";
    }
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::shared_ptr<error::error_info> p{};
    test_statement("insert into T0(C0, C1) values (1, 10.0)");

    static constexpr std::size_t num_thread = 5;
    std::vector<std::future<void>> vec{};
    utils::latch start{};
    for(std::size_t i=0; i < num_thread; ++i) {
        vec.emplace_back(
            std::async(std::launch::async, [&, i]() {
                start.wait();
                test_query();
            })
        );
    }
    std::this_thread::sleep_for(1ms);
    start.release();
    for(auto&& x : vec) {
        (void)x.get();
    }
}

TEST_F(service_api_test, query_unauthorized) {
    execute_statement("create table t (c0 int primary key)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);

    auto s = encode_execute_query(tx_handle, "select * from t");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    req->session_info_.user_type_ = user_type::standard;
    req->session_info_.username_ = "user1";
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_TRUE(res->all_released());
    EXPECT_EQ(tateyama::proto::diagnostics::Code::PERMISSION_ERROR, res->error_.code());

    // verify inactive
    test_commit(tx_handle, true, error_code::inactive_transaction_exception); // verify tx already aborted
}

TEST_F(service_api_test, statement_unauthorized) {
    execute_statement("create table t (c0 int primary key)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);

    auto s = encode_execute_statement(tx_handle, "insert into t values (1)");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    req->session_info_.user_type_ = user_type::standard;
    req->session_info_.username_ = "user1";
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_TRUE(res->all_released());
    EXPECT_EQ(tateyama::proto::diagnostics::Code::PERMISSION_ERROR, res->error_.code());

    // verify inactive
    test_commit(tx_handle, true, error_code::inactive_transaction_exception); // verify tx already aborted
}

TEST_F(service_api_test, get_transaction_status) {
    // verify transaction status using service api
    // the test depends on the timing, so only a few status can be verified
    using ts = ::jogasaki::proto::sql::response::TransactionStatus;
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_get_tx_status(tx_handle, ts::RUNNING);
    test_commit(tx_handle, false); // auto_dispose = false
    wait_epochs();
    test_get_tx_status(tx_handle, ts::STORED);
}

TEST_F(service_api_test, get_transaction_status_auto_dispose) {
    // same as get_transaction_status, but auto_dispose = true that causes transaction to be disposed after successful commit
    using ts = ::jogasaki::proto::sql::response::TransactionStatus;
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_get_tx_status(tx_handle, ts::RUNNING);
    test_commit(tx_handle);
    wait_epochs();
    test_get_tx_status(tx_handle, std::nullopt, error_code::transaction_not_found_exception);
}

TEST_F(service_api_test, get_transaction_status_updated_internally) {
    // verify transaction status by modifying it via internal api
    global::config_pool()->enable_session_store(true);
    using ts = ::jogasaki::proto::sql::response::TransactionStatus;
    api::transaction_handle tx_handle{};
    {
        test_begin(tx_handle);
        test_get_tx_status(tx_handle, ts::RUNNING);
        auto tctx = get_transaction_context(tx_handle);
        tctx->state(transaction_state_kind::going_to_commit);
        test_get_tx_status(tx_handle, ts::COMMITTING);
        tctx->state(transaction_state_kind::cc_committing);
        test_get_tx_status(tx_handle, ts::COMMITTING);
        tctx->state(transaction_state_kind::committed_available);
        test_get_tx_status(tx_handle, ts::AVAILABLE);
        tctx->state(transaction_state_kind::committed_stored);
        test_get_tx_status(tx_handle, ts::STORED);
        (void) tctx->abort_transaction(); // just for cleanup
    }
    {
        test_begin(tx_handle);
        auto tctx = get_transaction_context(tx_handle);
        tctx->state(transaction_state_kind::going_to_abort);
        test_get_tx_status(tx_handle, ts::ABORTING);
        tctx->state(transaction_state_kind::aborted);
        test_get_tx_status(tx_handle, ts::ABORTED);
        tctx->state(transaction_state_kind::unknown);
        test_get_tx_status(tx_handle, ts::UNTRACKED);
        (void) tctx->abort_transaction(); // just for cleanup
    }
}

TEST_F(service_api_test, protobuf1) {
    // verify protobuf behavior
    using namespace std::string_view_literals;
    std::stringstream ss;
    sql::request::Request req{};
    EXPECT_FALSE(req.has_begin());
    EXPECT_FALSE(req.has_session_handle());
    auto& h = req.session_handle();
    EXPECT_EQ(0, h.handle());  // default object has zero handle, that means empty
    auto* session = req.mutable_session_handle();
    EXPECT_TRUE(req.has_session_handle());
    req.clear_session_handle();
    EXPECT_FALSE(req.has_session_handle());

    sql::common::Session s;
    req.set_allocated_session_handle(&s);
    EXPECT_TRUE(req.has_session_handle());

    ::google::protobuf::TextFormat::Printer printer{};
    printer.SetSingleLineMode(true);
    std::string out{};
    EXPECT_TRUE(printer.PrintToString(req, &out));
    std::cerr << "out: " << out << std::endl;
    EXPECT_FALSE(out.empty());

    (void) req.release_session_handle();
    EXPECT_FALSE(req.has_session_handle());
}

TEST_F(service_api_test, invalid_request) {
    // error returned as parse error
    auto req = std::make_shared<tateyama::api::server::mock::test_request>("ABC", session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
}

TEST_F(service_api_test, empty_request) {
    // error returned as "invalid request code"
    auto req = std::make_shared<tateyama::api::server::mock::test_request>("", session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
}

TEST_F(service_api_test, syntax_error_aborts_tx) {
    api::transaction_handle tx_handle{};
    std::uint64_t stmt_handle{0};
    {
        test_begin(tx_handle);
        auto text = "select * from dummy"s;
        auto s = encode_execute_query(tx_handle, text);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_FALSE(success);
            ASSERT_EQ(error_code::symbol_analyze_exception, error.code_);
            ASSERT_FALSE(error.message_.empty());
        }
        test_commit(tx_handle, true, error_code::inactive_transaction_exception); // verify tx already aborted
    }
}

TEST_F(service_api_test, invalid_stmt_on_execute_prepared_statement_or_query) {
    api::transaction_handle tx_handle{};
    std::uint64_t stmt_handle{0};
    {
        test_begin(tx_handle);
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, stats] = decode_execute_result(res->body_);
        ASSERT_FALSE(success);
        ASSERT_EQ(error_code::sql_execution_exception, error.code_);
        ASSERT_FALSE(error.message_.empty());
        test_commit(tx_handle, true, error_code::inactive_transaction_exception); // verify tx already aborted
    }
    {
        test_begin(tx_handle);
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_FALSE(success);
        ASSERT_EQ(error_code::sql_execution_exception, error.code_);
        ASSERT_FALSE(error.message_.empty());
        test_rollback(tx_handle); // Even tx has been aborted already, requesting rollback is successful.
        //note that repeating rollback here results in segv because commit or rollback request destroys tx body and tx handle gets dangling
    }
}

TEST_F(service_api_test, execute_statement_as_query) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    execute_statement_as_query("insert into T0(C0, C1) values (1, 10.0)");
    execute_statement_as_query("update T0 set C1=20.0 where C0=1");
}

TEST_F(service_api_test, execute_query_as_statement) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    test_statement("insert into T0(C0, C1) values (1, 10.0)");
    test_statement("insert into T0(C0, C1) values (2, 20.0)");
    test_statement("insert into T0(C0, C1) values (3, 30.0)");
    test_statement("select * from T0");
}

TEST_F(service_api_test, null_host_variable) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
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
            {"c1"s, ValueCase::kFloat8Value, std::any{}},
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
    test_dispose_prepare(stmt_handle);
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM T0", result);
        ASSERT_EQ(1, result.size());
        auto& rec = result[0];
        EXPECT_FALSE(rec.is_null(0));
        EXPECT_EQ(1, rec.get_value<std::int64_t>(0));
        EXPECT_TRUE(rec.is_null(1));
    }
}

TEST_F(service_api_test, begin_long_tx) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    execute_statement("create table T1 (C0 int, C1 bigint, C2 double, C3 real, C4 varchar(100), primary key(C0, C1))");
    api::transaction_handle tx_handle{};
    {
        test_begin(tx_handle, false, true, {"T0", "T1"}, "mylabel");
        test_commit(tx_handle);
    }
    {
        test_begin(tx_handle, true, true, {}, "mylabel2");
        test_commit(tx_handle);
    }
}

TEST_F(service_api_test, long_tx_simple) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    api::transaction_handle tx_handle{};
    {
        test_begin(tx_handle, false, true, {"T0"});
        test_statement("insert into T0(C0, C1) values (1, 10.0)", tx_handle);
        test_query(
            "select * from T0 where C0=1",
            tx_handle,
            {
                {"C0", common_column::atom_type::int8},  // nullable is not sent now
                {"C1", common_column::atom_type::float8},  // nullable is not sent now
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
}

TEST_F(service_api_test, execute_ddl) {
    test_statement("create table MYTABLE(C0 bigint primary key, C1 double)");
    test_statement("insert into MYTABLE(C0, C1) values (1, 10.0)");
    test_query("select * from MYTABLE");
}

TEST_F(service_api_test, empty_result_set) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_query(
        "select * from T0",
        tx_handle,
        {
            {"C0", common_column::atom_type::int8},    // nullable is not sent now
            {"C1", common_column::atom_type::float8},   // nullable is not sent now
        },
        {
            true,
            true
        },
        {},
        {"C0", "C1"}
    );
    test_commit(tx_handle);
}

TEST_F(service_api_test, create_many_tx) {
    // verify there is neither resource leak nor lack of closing/destructing tx objects
    for(std::size_t i=0; i < 300; ++i) {
        api::transaction_handle tx_handle{};
        test_begin(tx_handle);
        test_commit(tx_handle);
    }
}

TEST_F(service_api_test, tx_id) {
    begin_result result{};
    test_begin(result);
    test_commit(result.handle_);
    EXPECT_FALSE(result.transaction_id_.empty());
    LOG(INFO) << "tx_id: " << result.transaction_id_;
}

bool contains(std::vector<std::string> const& v, std::string_view s) {
    auto it = v.begin();
    while(it != v.end()) {
        if(*it == s) {
            return true;
        }
        ++it;
    }
    return false;
}

TEST_F(service_api_test, list_tables) {
    execute_statement("create table TT0 (C0 INT)");
    execute_statement("create table TT1 (C0 INT)");
    execute_statement("create index II on TT0(C0)");
    auto s = encode_list_tables();
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto result = decode_list_tables(res->body_);
    ASSERT_TRUE(contains(result, "TT0"));
    ASSERT_TRUE(contains(result, "TT1"));
    ASSERT_FALSE(contains(result, "II"));
}

TEST_F(service_api_test, get_search_path) {
    auto s = encode_get_search_path();
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto result = decode_get_search_path(res->body_);
    ASSERT_EQ(0, result.size());
}

TEST_F(service_api_test, modifies_definitions) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle, false, true, {}, "modifies_definitions", true);
    test_statement("CREATE TABLE TT(C0 INT)", tx_handle);
    test_commit(tx_handle);
}

TEST_F(service_api_test, get_error_info) {
    // verify get error info is not affected by err_inactive_transaction (request failure, not transaction failure)
    test_statement("CREATE TABLE TT(C0 INT NOT NULL PRIMARY KEY)");
    test_statement("INSERT INTO TT VALUES (0)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_statement("INSERT INTO TT VALUES (0)", tx_handle, error_code::unique_constraint_violation_exception);
    test_statement("INSERT INTO TT VALUES (1)", tx_handle, error_code::inactive_transaction_exception);
    test_statement("INSERT INTO TT VALUES (2)", tx_handle, error_code::inactive_transaction_exception);
    test_get_error_info(tx_handle, false, error_code::unique_constraint_violation_exception);
    test_dispose_transaction(tx_handle);
}

TEST_F(service_api_test, dispose_transaction_invalid_handle) {
    test_dispose_transaction(api::transaction_handle{1});  // disposing invalid handle is no-op
}

TEST_F(service_api_test, dispose_transaction_missing_handle) {
    // protobuf treats 0 as if not handle is specified
    // this case is handled as an error because sending 0 is usage error anyway
    test_dispose_transaction({}, error_code::sql_execution_exception);
}

TEST_F(service_api_test, dispose_transaction) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot spwan multiple transactions";
    }
    api::transaction_handle tx_handle0{};
    test_begin(tx_handle0);
    api::transaction_handle tx_handle1{};
    test_begin(tx_handle1);

    EXPECT_EQ(2, get_impl(*db_).transaction_count());
    test_dispose_transaction(tx_handle0);
    EXPECT_EQ(1, get_impl(*db_).transaction_count());
    test_dispose_transaction(tx_handle1);
    EXPECT_EQ(0, get_impl(*db_).transaction_count());
}

TEST_F(service_api_test, dispose_transaction_aborted) {
    // verify aborted tx is left on db
    test_statement("CREATE TABLE TT(C0 INT NOT NULL PRIMARY KEY)");
    test_statement("INSERT INTO TT VALUES (0)");
    {
        api::transaction_handle tx_handle{};
        test_begin(tx_handle);
        test_statement("INSERT INTO TT VALUES (0)", tx_handle, error_code::unique_constraint_violation_exception);

        EXPECT_EQ(1, get_impl(*db_).transaction_count());
        test_dispose_transaction(tx_handle);
        EXPECT_EQ(0, get_impl(*db_).transaction_count());
    }
}

TEST_F(service_api_test, dispose_transaction_auto_dispose) {
    // committed tx is automatically disposed
    test_statement("CREATE TABLE TT(C0 INT NOT NULL PRIMARY KEY)");
    test_statement("INSERT INTO TT VALUES (0)");
    {
        api::transaction_handle tx_handle{};
        test_begin(tx_handle);
        test_statement("INSERT INTO TT VALUES (1)", tx_handle);
        test_commit(tx_handle);

        EXPECT_EQ(0, get_impl(*db_).transaction_count());
        test_dispose_transaction(tx_handle); // this is no-op
    }
}

TEST_F(service_api_test, get_error_info_on_compile_error) {
    // verify get error info with compile error
    test_statement("CREATE TABLE TT(C0 INT NOT NULL PRIMARY KEY)");
    test_statement("INSERT INTO TT VALUES (0)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_statement("INSERT INTO dummy VALUES (0)", tx_handle, error_code::symbol_analyze_exception);
    test_statement("INSERT INTO TT VALUES (1)", tx_handle, error_code::inactive_transaction_exception);
    test_get_error_info(tx_handle, false, error_code::symbol_analyze_exception);
    test_dispose_transaction(tx_handle);
}

TEST_F(service_api_test, get_error_info_on_empty_commit) {
    // verify get error info sees tx not found after successful commit (auto disposed)
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_commit(tx_handle);
    test_get_error_info(tx_handle, true, error_code::transaction_not_found_exception);
}

TEST_F(service_api_test, get_error_info_on_empty_commit_auto_dispose_off) {
    // verify get error info sees error not found (requires auto dispose off to avoid getting disposed very soon)
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_commit(tx_handle, false);
    test_get_error_info(tx_handle, false, error_code::none);
}

TEST_F(service_api_test, stats) {
    test_statement("create table T(C0 int primary key)");
    test_statement("insert into T values (0)");
    test_statement("insert into T values (2)");
    {
        std::shared_ptr<request_statistics> stats{};
        test_statement("insert into T values (1)", stats);
        ASSERT_TRUE(stats);
        EXPECT_EQ(1, stats->counter(counter_kind::inserted).count());
    }
    {
        std::shared_ptr<request_statistics> stats{};
        test_statement("update T set C0=0 where C0=0", stats);
        ASSERT_TRUE(stats);
        EXPECT_EQ(1, stats->counter(counter_kind::updated).count());
    }
    {
        std::shared_ptr<request_statistics> stats{};
        test_statement("delete from T where C0=2", stats);
        ASSERT_TRUE(stats);
        EXPECT_EQ(1, stats->counter(counter_kind::deleted).count());
    }
    {
        std::shared_ptr<request_statistics> stats{};
        test_statement("insert or replace into T values (3)", stats);
        ASSERT_TRUE(stats);
        EXPECT_EQ(1, stats->counter(counter_kind::merged).count());
    }
}
TEST_F(service_api_test, stats_wo_change) {
    test_statement("create table T(C0 int primary key)");
    test_statement("insert into T values (0)");
    {
        std::shared_ptr<request_statistics> stats{};
        test_statement("select * from T", stats);
        ASSERT_TRUE(stats);
        EXPECT_FALSE(stats->counter(counter_kind::inserted).has_value());
        EXPECT_FALSE(stats->counter(counter_kind::updated).has_value());
        EXPECT_FALSE(stats->counter(counter_kind::merged).has_value());
        EXPECT_FALSE(stats->counter(counter_kind::deleted).has_value());
    }
    {
        std::shared_ptr<request_statistics> stats{};
        test_statement("insert if not exists into T values (0)", stats);
        ASSERT_TRUE(stats);
        EXPECT_EQ(0, stats->counter(counter_kind::inserted).count());
    }
    {
        std::shared_ptr<request_statistics> stats{};
        test_statement("update T set C0=0 where C0=10", stats);
        ASSERT_TRUE(stats);
        EXPECT_EQ(0, stats->counter(counter_kind::updated).count());
    }
    {
        std::shared_ptr<request_statistics> stats{};
        test_statement("delete from T where C0=10", stats);
        ASSERT_TRUE(stats);
        EXPECT_EQ(0, stats->counter(counter_kind::deleted).count());
    }
}

TEST_F(service_api_test, batch_unsupported) {
    auto s = encode_batch();
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto err = res->error_;
    ASSERT_EQ(::tateyama::proto::diagnostics::Code::UNSUPPORTED_OPERATION, err.code());
}

TEST_F(service_api_test, error_with_unsupported_query) {
    // verify error occurring during task creation is correctly handled
    // bad way of setting error_info on request_context failed to set status code correctly (use error::set_error_info)
    execute_statement("CREATE TABLE t (c0 int)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_statement("SELECT count(c0), count(DISTINCT c0) from t", tx_handle, error_code::unsupported_runtime_feature_exception);
    test_get_error_info(tx_handle, false, error_code::unsupported_runtime_feature_exception);
    test_dispose_transaction(tx_handle);
}

TEST_F(service_api_test, extract_sql_info) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    global::config_pool()->enable_session_store(true);
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    auto text = "select C0, C1 from T0 where C0 = 1 and C1 = 1.0"s;
    auto query = encode_execute_query(tx_handle, text);

    auto s = encode_extract_statement_info(query, session_id_);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, tx_id, error] = decode_extract_statement_info(res->body_);
    ASSERT_FALSE(result.empty());
    ASSERT_FALSE(tx_id.empty());
    EXPECT_EQ(text, result);
}

TEST_F(service_api_test, extract_sql_info_missing_statement) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    api::transaction_handle tx_handle{};
    auto text = "select C0, C1 from T0 where C0 = 1 and C1 = 1.0"s;

    std::uint64_t stmt_handle{};
    auto query = encode_execute_prepared_statement(tx_handle, stmt_handle, {});

    auto s = encode_extract_statement_info(query);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, tx_id, error] = decode_extract_statement_info(res->body_);
    ASSERT_TRUE(result.empty());
    ASSERT_TRUE(tx_id.empty());
    EXPECT_EQ(error_code::statement_not_found_exception, error.code_);
}

TEST_F(service_api_test, extract_sql_prepared_on_different_session) {
    // verify prepared statement and tx that are associated on session 1000 can be extracted on session 2000
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    global::config_pool()->enable_session_store(true);
    auto text = "select C0, C1 from T0 where C0 = 1 and C1 = 1.0"s;
    session_id_ = 1000;
    std::uint64_t stmt_handle{};
    test_prepare(stmt_handle, text);

    api::transaction_handle tx_handle{};
    test_begin(tx_handle);

    auto query = encode_execute_prepared_query(tx_handle, stmt_handle, {});

    session_id_ = 2000;
    auto s = encode_extract_statement_info(query, 1000);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);

    auto [result, tx_id, error] = decode_extract_statement_info(res->body_);
    ASSERT_FALSE(result.empty());
    ASSERT_FALSE(tx_id.empty());
    EXPECT_EQ(text, result);
}

TEST_F(service_api_test, use_tx_on_different_session) {
    // verify handle is not usable on different session
    global::config_pool()->enable_session_store(true);
    test_statement("CREATE TABLE TT(C0 INT NOT NULL PRIMARY KEY)");
    test_statement("INSERT INTO TT VALUES (0)");

    session_id_ = 1000;
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_statement("INSERT INTO TT VALUES (1)", tx_handle);

    session_id_ = 2000;
    api::transaction_handle tx_handle2{tx_handle.surrogate_id(), session_id_};
    // test_stmt_err uses api::get_transaction_context() in test tool and does not test behavior of service api correctly
    test_statement("INSERT INTO TT VALUES (2)", tx_handle2, error_code::transaction_not_found_exception);

    session_id_ = 1000;
    test_statement("INSERT INTO TT VALUES (3)", tx_handle);

    test_commit(tx_handle, false);

    test_dispose_transaction(tx_handle);
}

TEST_F(service_api_test, statement_on_different_session) {
    // verify handle is not usable on different session
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    global::config_pool()->enable_session_store(true);
    session_id_ = 1000;
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0 (C0, C1) values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::INT8},
        std::pair{"p1"s, sql::common::AtomType::FLOAT8}
    );

    session_id_ = 2000;

    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_prepared_statement(stmt_handle, tx_handle, error_code::statement_not_found_exception);
    // test_commit(tx_handle, true); // tx already aborted by the error above
    test_dispose_transaction(tx_handle);

    session_id_ = 1000;
    api::transaction_handle tx_handle2{};
    test_begin(tx_handle2);
    test_prepared_statement(stmt_handle, tx_handle2);
    test_commit(tx_handle2, false);

    test_dispose_transaction(tx_handle2);
    test_dispose_prepare(stmt_handle);
}

TEST_F(service_api_test, disposing_statement_twice) {
    // verify there is no error returned when diposing the invalid statement handle
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0 (C0, C1) values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::INT8},
        std::pair{"p1"s, sql::common::AtomType::FLOAT8}
    );
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(stmt_handle);
}

}  // namespace jogasaki::api
