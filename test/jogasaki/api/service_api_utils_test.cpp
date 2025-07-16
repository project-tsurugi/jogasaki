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
#include <jogasaki/constants.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
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

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::string_view_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::utils;
using namespace jogasaki::model;
using namespace jogasaki::executor;
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

class service_api_utils_test :
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
        cfg->enable_session_store(true);
        set_dbpath(*cfg);

        db_ = std::shared_ptr{jogasaki::api::create_database(cfg)};
        auto c = std::make_shared<tateyama::api::configuration::whole>("");
        service_ = std::make_shared<jogasaki::api::impl::service>(c, db_.get());
        db_->start();

        auto* impl = db_impl();
        utils::add_test_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());

        utils::utils_raise_exception_on_error = true;
        temporary_.prepare();
    }

    void TearDown() override {
        db_teardown();
        temporary_.clean();
    }
    void test_begin(api::transaction_handle& handle,
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
        api::transaction_handle& handle,
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
        std::vector<sql::common::AtomType> const& column_types,
        std::vector<bool> const& nullabilities,
        std::vector<mock::basic_record> const& expected,
        std::vector<std::string> const& exp_colnames
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
    void test_load(bool transactional, error_code expected, Args ... args);

    void execute_statement_as_query(std::string_view sql);

    std::shared_ptr<jogasaki::api::impl::service> service_{};  //NOLINT
    test::temporary_folder temporary_{};
    std::size_t session_id_{100};

};


void service_api_utils_test::test_begin(api::transaction_handle& tx_handle,
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

void service_api_utils_test::test_begin(
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

void service_api_utils_test::test_commit(
    transaction_handle& tx_handle,
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

void service_api_utils_test::test_rollback(api::transaction_handle& tx_handle) {
    auto s = encode_rollback(tx_handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);
    auto [success, error] = decode_result_only(res->body_);
    ASSERT_TRUE(success);
}

void service_api_utils_test::test_dispose_prepare(std::uint64_t handle) {
    auto s = encode_dispose_prepare(handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    auto [success, error] = decode_result_only(res->body_);
    ASSERT_TRUE(success);
}

void service_api_utils_test::test_statement(
    std::string_view sql, api::transaction_handle tx_handle, error_code exp) {
    std::shared_ptr<request_statistics> stats{};
    test_statement(sql, tx_handle, exp, stats);
}

void service_api_utils_test::test_statement(
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
void service_api_utils_test::test_statement(std::string_view sql, api::transaction_handle tx_handle) {
    test_statement(sql, tx_handle, error_code::none);
}

void service_api_utils_test::test_statement(std::string_view sql, api::transaction_handle tx_handle, std::shared_ptr<request_statistics>& stats) {
    test_statement(sql, tx_handle, error_code::none, stats);
}

void service_api_utils_test::test_statement(std::string_view sql) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_statement(sql, tx_handle);
    test_commit(tx_handle);
}

void service_api_utils_test::test_statement(std::string_view sql, std::shared_ptr<request_statistics>& stats) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_statement(sql, tx_handle, stats);
    test_commit(tx_handle);
}

void service_api_utils_test::test_query(
    std::string_view sql,
    api::transaction_handle tx_handle,
    std::vector<sql::common::AtomType> const& column_types,
    std::vector<bool> const& nullabilities,
    std::vector<mock::basic_record> const& expected,
    std::vector<std::string> const& exp_colnames
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
        std::cout << "name : " << name << std::endl;
        ASSERT_EQ(column_types.size(), cols.size());

        for(std::size_t i=0, n=cols.size(); i<n; ++i) {
            EXPECT_EQ(column_types[i], cols[i].type_);
            EXPECT_TRUE(! cols[i].nullable_.has_value());
            EXPECT_EQ(exp_colnames[i], cols[i].name_);
        }
        {
            ASSERT_TRUE(res->channel_);
            auto& ch = *res->channel_;
            auto m = create_record_meta(cols);
            auto v = deserialize_msg(ch.view(), m);
            ASSERT_EQ(expected.size(), v.size());
            for(std::size_t i=0, n=v.size(); i<n; ++i) {
                EXPECT_EQ(expected[i], v[i]);
            }
            EXPECT_TRUE(ch.all_released());
        }
    }
    {
        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
}

void service_api_utils_test::test_query(std::string_view query) {
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_query(
        query,
        tx_handle,
        {
            sql::common::AtomType::INT8,
            sql::common::AtomType::FLOAT8
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

void service_api_utils_test::execute_statement_as_query(std::string_view sql) {
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

void service_api_utils_test::test_dump(std::vector<std::string>& files, std::string_view dir, error_code expected) {
    std::string p{dir.empty() ? service_api_utils_test::temporary_.path() : std::string{dir}};
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
            std::cout << "name : " << name << std::endl;
            ASSERT_EQ(1, cols.size());
            EXPECT_EQ(sql::common::AtomType::CHARACTER, cols[0].type_);
            EXPECT_TRUE(! cols[0].nullable_.has_value());
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

template <class ... Args>
void service_api_utils_test::test_load(bool transactional, error_code expected, Args...files) {
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0 (C0, C1) values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::INT8},
        std::pair{"p1"s, sql::common::AtomType::FLOAT8}
    );
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
    test_dispose_prepare(stmt_handle);
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

void service_api_utils_test::test_dispose_transaction(
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

void service_api_utils_test::test_cancel_transaction_commit(api::transaction_handle tx_handle, bool auto_dispose_on_commit_success) {
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

void service_api_utils_test::test_cancel_transaction_begin(api::transaction_handle tx_handle, std::string_view label) {
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

void service_api_utils_test::test_cancel_statement(
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

TEST_F(service_api_utils_test, extract_sql) {
    {
        // non-prepared statement
        auto text = "insert into T0 values (1,1)"s;

        api::transaction_handle tx_handle{};
        test_begin(tx_handle);
        std::vector<parameter> parameters{};
        auto s = encode_execute_statement(tx_handle, text);

        sql::request::Request req{};
        utils::deserialize(s, req);

        std::shared_ptr<std::string> sql_text{};
        std::shared_ptr<error::error_info> err_info{};
        std::string tx_id{};

        ASSERT_TRUE(impl::extract_sql_and_tx_id(req, sql_text, tx_id, err_info, session_id_));
        ASSERT_TRUE(sql_text);
        EXPECT_EQ(text, *sql_text);
        EXPECT_TRUE(! tx_id.empty()) << "tx_id:" << tx_id;
        test_commit(tx_handle);
    }
    {
        // non-prepared query
        auto text = "select * from T1"s;

        api::transaction_handle tx_handle{};
        test_begin(tx_handle);
        std::vector<parameter> parameters{};
        auto s = encode_execute_query(tx_handle, text);

        sql::request::Request req{};
        utils::deserialize(s, req);

        std::shared_ptr<std::string> sql_text{};
        std::shared_ptr<error::error_info> err_info{};
        std::string tx_id{};
        ASSERT_TRUE(impl::extract_sql_and_tx_id(req, sql_text, tx_id, err_info, session_id_));
        ASSERT_TRUE(sql_text);
        EXPECT_EQ(text, *sql_text);
        EXPECT_TRUE(! tx_id.empty()) << "tx_id:" << tx_id;
        test_commit(tx_handle);
    }
}

TEST_F(service_api_utils_test, extract_prepared_sql) {
    {
        // prepared statement
        statement_handle stmt_handle{};
        auto text = "insert into T0 values (1,1)"s;
        test_prepare(stmt_handle, text);

        api::transaction_handle tx_handle{};
        test_begin(tx_handle);
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle.get(), parameters);

        sql::request::Request req{};
        utils::deserialize(s, req);

        std::shared_ptr<std::string> sql_text{};
        std::shared_ptr<error::error_info> err_info{};
        std::string tx_id{};
        ASSERT_TRUE(impl::extract_sql_and_tx_id(req, sql_text, tx_id, err_info, session_id_));
        ASSERT_TRUE(sql_text);
        EXPECT_EQ(text, *sql_text);
        EXPECT_TRUE(! tx_id.empty()) << "tx_id:" << tx_id;

        test_commit(tx_handle);
    }
    {
        // prepared query
        statement_handle stmt_handle{};
        auto text = "select * from T1"s;
        test_prepare(stmt_handle, text);

        api::transaction_handle tx_handle{};
        test_begin(tx_handle);
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, stmt_handle.get(), parameters);

        sql::request::Request req{};
        utils::deserialize(s, req);

        std::shared_ptr<std::string> sql_text{};
        std::shared_ptr<error::error_info> err_info{};
        std::string tx_id{};
        ASSERT_TRUE(impl::extract_sql_and_tx_id(req, sql_text, tx_id, err_info, session_id_));
        ASSERT_TRUE(sql_text);
        EXPECT_EQ(text, *sql_text);
        EXPECT_TRUE(! tx_id.empty()) << "tx_id:" << tx_id;

        test_commit(tx_handle);
        test_dispose_prepare(stmt_handle.get());
    }
}

TEST_F(service_api_utils_test, extract_sql_error) {
    // verify error with unsupported sql messages

    api::transaction_handle tx_handle{};
    auto s = encode_commit(tx_handle, true);

    sql::request::Request req{};
    utils::deserialize(s, req);

    std::shared_ptr<std::string> sql_text{};
    std::string tx_id{};
    std::shared_ptr<error::error_info> err_info{};
    ASSERT_TRUE(! impl::extract_sql_and_tx_id(req, sql_text, tx_id, err_info, session_id_));
    ASSERT_TRUE(err_info);
    EXPECT_EQ(error_code::request_failure_exception, err_info->code());
}

TEST_F(service_api_utils_test, extract_sql_failing_to_fetch_tx_id) {
    // depending on timing, transaction_context already disposed and empty tx_id is returned

    std::uint64_t stmt_handle{};
    auto text = "select * from T1"s;
    test_prepare(stmt_handle, text);

    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_commit(tx_handle, true);

    std::vector<parameter> parameters{};
    auto s = encode_execute_prepared_query(tx_handle, stmt_handle, parameters);

    sql::request::Request req{};
    utils::deserialize(s, req);

    std::shared_ptr<std::string> sql_text{};
    std::shared_ptr<error::error_info> err_info{};
    std::string tx_id{};
    ASSERT_TRUE(impl::extract_sql_and_tx_id(req, sql_text, tx_id, err_info, session_id_));
    ASSERT_TRUE(sql_text);
    EXPECT_EQ(text, *sql_text);
    EXPECT_TRUE(tx_id.empty());

    test_dispose_prepare(stmt_handle);
}

TEST_F(service_api_utils_test, fail_to_extract_sql_on_different_session) {
    // statement prepared on session 100, transaction began on session 1000, extract requested on 1000

    session_id_ = 100;
    std::uint64_t stmt_handle{};
    auto text = "select * from T1"s;
    test_prepare(stmt_handle, text);

    session_id_ = 1000;
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);

    std::vector<parameter> parameters{};
    auto s = encode_execute_prepared_query(tx_handle, stmt_handle, parameters);

    sql::request::Request req{};
    utils::deserialize(s, req);

    std::shared_ptr<std::string> sql_text{};
    std::shared_ptr<error::error_info> err_info{};
    std::string tx_id{};
    ASSERT_TRUE(! impl::extract_sql_and_tx_id(req, sql_text, tx_id, err_info, session_id_));
    ASSERT_TRUE(err_info);
    EXPECT_EQ(error_code::statement_not_found_exception, err_info->code());

    test_dispose_prepare(stmt_handle);
    test_commit(tx_handle, false);
    test_dispose_transaction(tx_handle);
}

TEST_F(service_api_utils_test, fail_to_extract_tx_on_different_session) {
    // tx began on session 100 but statement prepared on session 1000, extract requested on 1000
    // contrary to statement, this is not an error because depending on timing tx has been disposed and empty tx_id is returned
    session_id_ = 100;
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);

    session_id_ = 1000;
    std::uint64_t stmt_handle{};
    auto text = "select * from T1"s;
    test_prepare(stmt_handle, text);

    std::vector<parameter> parameters{};
    auto s = encode_execute_prepared_query(tx_handle, stmt_handle, parameters);

    sql::request::Request req{};
    utils::deserialize(s, req);

    std::shared_ptr<std::string> sql_text{};
    std::shared_ptr<error::error_info> err_info{};
    std::string tx_id{};
    ASSERT_TRUE(impl::extract_sql_and_tx_id(req, sql_text, tx_id, err_info, session_id_));
    ASSERT_TRUE(! err_info);
    ASSERT_TRUE(sql_text);
    EXPECT_EQ(text, *sql_text);
    EXPECT_TRUE(tx_id.empty());

    test_dispose_prepare(stmt_handle);

    session_id_ = 100;
    test_commit(tx_handle, false);
    test_dispose_transaction(tx_handle);
}

}  // namespace jogasaki::api
