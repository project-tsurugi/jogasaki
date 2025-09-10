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
#pragma once

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

class service_api_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->skip_smv_check(true); // for testing, we don't check message versions
        set_dbpath(*cfg);

        db_ = std::shared_ptr{jogasaki::api::create_database(cfg)};
        auto c = std::make_shared<tateyama::api::configuration::whole>("");
        service_ = std::make_shared<jogasaki::api::impl::service>(c, db_.get());
        db_->start();

        utils::utils_raise_exception_on_error = true;
        temporary_.prepare();
        datastore::get_datastore(true); // in case mixing mock/prod datastore
    }

    void TearDown() override {
        db_teardown();
        temporary_.clean();
    }
    void test_begin(
        api::transaction_handle& tx_handle,
        bool readonly = false,
        bool is_long = false,
        std::vector<std::string> const& write_preserves = {},
        std::string_view label = {},
        bool modifies_definitions = false
    );
    void test_begin(begin_result& result,
        bool readonly = false,
        bool is_long = false,
        std::vector<std::string> const& write_preserves = {},
        std::string_view label = {},
        bool modifies_definitions = false
    );
    void test_commit(
        api::transaction_handle& tx_handle,
        bool auto_dispose_on_commit_success = true,
        error_code expected = error_code::none
    );
    void test_dispose_transaction(api::transaction_handle tx_handle, error_code expected = error_code::none);
    void test_rollback(api::transaction_handle& tx_handle);
    void test_statement(std::string_view sql);
    void test_statement(std::string_view sql, std::shared_ptr<request_statistics>& stats);
    void test_statement(std::string_view sql, api::transaction_handle tx_handle);
    void test_statement(std::string_view sql, api::transaction_handle tx_handle, std::shared_ptr<request_statistics>& stats);
    void test_statement(std::string_view sql, api::transaction_handle tx_handle, error_code exp);
    void test_statement(
        std::string_view sql, api::transaction_handle tx_handle, error_code exp, std::shared_ptr<request_statistics>& stats);
    void test_query(std::string_view query = "select * from T0");

    void test_query(
        std::string_view sql,
        api::transaction_handle tx_handle,
        std::vector<dto::common_column> const& column_types,
        std::vector<bool> const& nullabilities,
        std::vector<mock::basic_record> const& expected,
        std::vector<std::string> const& exp_colnames
    );
    void test_prepared_statement(std::uint64_t stmt_handle, api::transaction_handle tx_handle, error_code exp = error_code::none);
    void test_prepared_statement(
        std::uint64_t stmt_handle,
        api::transaction_handle tx_handle,
        error_code exp,
        std::shared_ptr<request_statistics>& stats
    );
    void test_cancel_statement(std::string_view sql, api::transaction_handle tx_handle);
    void test_cancel_transaction_begin(api::transaction_handle tx_handle, std::string_view label);
    void test_cancel_transaction_commit(api::transaction_handle tx_handle, bool auto_dispose_on_commit_success);

    template <class ...Args>
    void test_prepare(std::uint64_t& handle, std::string sql, Args...args) {
        auto s = encode_prepare(sql, args...);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        handle = decode_prepare(res->body_);
    }
    template <class ...Args>
    void test_prepare(statement_handle& handle, std::string sql, Args...args) {
        std::uint64_t sid{};
        test_prepare(sid, sql, args...);
        handle = {reinterpret_cast<void*>(sid), session_id_};
    }

    void test_dispose_prepare(std::uint64_t handle);

    template <class ...Args>
    void test_error_prepare(std::uint64_t& handle, std::string sql, Args...args) {
        auto s = encode_prepare(sql, args...);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        EXPECT_EQ(-1, decode_prepare(res->body_));
    }
    void test_get_error_info(
        api::transaction_handle tx_handle,
        bool expect_error, // expecting error occuring in GetErrorInfo
        error_code expected // common for both error occuring in GetErrorInfo and already executed request
    ) {
        auto s = encode_get_error_info(tx_handle);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error] = decode_get_error_info(res->body_);
        EXPECT_TRUE(res->all_released());

        if(!expect_error) {
            ASSERT_TRUE(success);
        } else {
            ASSERT_FALSE(success);
        }
        EXPECT_EQ(expected, error.code_);
        LOG(INFO) << "error message: " << error.message_;
        LOG(INFO) << "error supplemental text : " << error.supplemental_text_;
    }

    void test_dump(std::vector<std::string>& files, std::string_view dir = "", error_code expected = error_code::none);

    template <class ... Args>
    void test_load(bool transactional, error_code expected, Args...files) {
        std::uint64_t stmt_handle{};
        test_prepare(
            stmt_handle,
            "insert into T0 (C0, C1) values (:p0, :p1)",
            std::pair{"p0"s, sql::common::AtomType::INT8},
            std::pair{"p1"s, sql::common::AtomType::FLOAT8}
        );
        test_load(transactional, stmt_handle, expected, files...);
        test_dispose_prepare(stmt_handle);
    }

    template <class ... Args>
    void test_load(bool transactional, std::uint64_t& stmt_handle, error_code expected, Args...files) {
        api::transaction_handle tx_handle{};
        if(transactional) {
            test_begin(tx_handle);
        }
        {
            std::vector<parameter> parameters{
                {"p0"s, ValueCase::kReferenceColumnName, std::any{std::in_place_type<std::string>, "C0"}},
                {"p1"s, ValueCase::kReferenceColumnPosition, std::any{std::in_place_type<std::uint64_t>, 1}},
            };
            auto s = encode_execute_load(tx_handle, stmt_handle, parameters, files...);

            auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
            auto res = std::make_shared<tateyama::api::server::mock::test_response>();

            auto st = (*service_)(req, res);
            EXPECT_TRUE(res->wait_completion());
            EXPECT_TRUE(res->completed());
            EXPECT_TRUE(res->all_released());
            ASSERT_TRUE(st);
            {
                auto [success, error, stats] = decode_execute_result(res->body_);
                if(expected == error_code::none) {
                    ASSERT_TRUE(success);
                    if(transactional) {
                        test_commit(tx_handle);
                    }
                } else {
                    ASSERT_FALSE(success);
                    ASSERT_EQ(expected, error.code_);
                }
            }
        }
    }

    void test_get_lob(std::uint64_t id, std::string_view expected_path);

    void test_get_tx_status(api::transaction_handle tx_handle, std::optional<::jogasaki::proto::sql::response::TransactionStatus> expected_status, error_code expected_err = error_code::none);

    void execute_statement_as_query(std::string_view sql);

    std::shared_ptr<jogasaki::api::impl::service> service_{};  //NOLINT
    test::temporary_folder temporary_{};
    std::size_t session_id_{100};

};

void enable_request_cancel(request_cancel_kind kind);

}  // namespace jogasaki::api
