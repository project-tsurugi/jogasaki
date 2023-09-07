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

#include <sstream>
#include <future>
#include <thread>
#include <gtest/gtest.h>
#include <google/protobuf/text_format.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/decimal/triple.h>
#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>

#include <jogasaki/kvs/id.h>
#include <jogasaki/constants.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/latch.h>
#include <jogasaki/test_utils/temporary_folder.h>

#include <tateyama/api/server/mock/request_response.h>
#include <tateyama/api/server/response_code.h>
#include "api_test_base.h"
#include <jogasaki/utils/msgbuf_utils.h>

#include "jogasaki/proto/sql/request.pb.h"
#include "jogasaki/proto/sql/response.pb.h"
#include "jogasaki/proto/sql/common.pb.h"
#include "jogasaki/proto/sql/status.pb.h"

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

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
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
        cfg->prepare_test_tables(true);
        set_dbpath(*cfg);

        db_ = std::shared_ptr{jogasaki::api::create_database(cfg)};
        auto c = std::make_shared<tateyama::api::configuration::whole>("");
        service_ = std::make_shared<jogasaki::api::impl::service>(c, db_.get());
        db_->start();

        utils::utils_raise_exception_on_error = true;
        temporary_.prepare();
    }

    void TearDown() override {
        db_teardown();
        temporary_.clean();
    }
    void test_begin(std::uint64_t& handle,
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
        std::uint64_t& handle,
        bool auto_dispose_on_commit_success = true,
        error_code expected = error_code::none
    );
    void test_dispose_transaction(std::uint64_t handle, status expected = status::ok);
    void test_rollback(std::uint64_t& handle);
    void test_statement(std::string_view sql);
    void test_statement(std::string_view sql, std::uint64_t tx_handle);
    void test_statement(std::string_view sql, std::uint64_t tx_handle, status exp);
    void test_query(std::string_view query = "select * from T0");

    void test_query(
        std::string_view sql,
        std::uint64_t tx_handle,
        std::vector<sql::common::AtomType> const& column_types,
        std::vector<bool> const& nullabilities,
        std::vector<mock::basic_record> const& expected,
        std::vector<std::string> const& exp_colnames
    );

    template <class ...Args>
    void test_prepare(std::uint64_t& handle, std::string sql, Args...args) {
        auto s = encode_prepare(sql, args...);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);
        handle = decode_prepare(res->body_);
    }
    void test_dispose_prepare(std::uint64_t& handle);

    template <class ...Args>
    void test_error_prepare(std::uint64_t& handle, std::string sql) {
        auto s = encode_prepare(sql);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::application_error, res->code_);
        EXPECT_EQ(-1, decode_prepare(res->body_));
    }
    void test_get_error_info(
        std::uint64_t handle,
        error_code expected
    ) {
        auto s = encode_get_error_info(handle);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [success, error] = decode_get_error_info(res->body_);
        ASSERT_TRUE(success);
        EXPECT_EQ(expected, error.code_);
        LOG(INFO) << "error message: " << error.message_;
//        LOG(INFO) << "error supplemental text : " << error.supplemental_text_;
    }

    void test_dump(std::vector<std::string>& files, std::string_view dir = "", status expected = status::ok);

    template <class ... Args>
    void test_load(bool transactional, status expected, Args ... args);

    void execute_statement_as_query(std::string_view sql);

    std::shared_ptr<jogasaki::api::impl::service> service_{};  //NOLINT
    test::temporary_folder temporary_{};

};


void service_api_test::test_begin(std::uint64_t& handle,
    bool readonly,
    bool is_long,
    std::vector<std::string> const& write_preserves,
    std::string_view label,
    bool modifies_definitions
) {
    begin_result result{};
    test_begin(result, readonly, is_long, write_preserves, label, modifies_definitions);
    handle = result.handle_;
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
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);
    result = decode_begin(res->body_);
}

void service_api_test::test_commit(
    std::uint64_t& handle,
    bool auto_dispose_on_commit_success,
    error_code expected
) {
    auto s = encode_commit(handle, auto_dispose_on_commit_success);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);

    ASSERT_EQ(expected == error_code::none ? response_code::success : response_code::application_error, res->code_);
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

void service_api_test::test_rollback(std::uint64_t& handle) {
    auto s = encode_rollback(handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);
    auto [success, error] = decode_result_only(res->body_);
    ASSERT_TRUE(success);
}

TEST_F(service_api_test, begin_and_commit) {
    std::uint64_t handle{};
    test_begin(handle);
    test_commit(handle);
}

TEST_F(service_api_test, error_on_commit) {
    std::uint64_t handle{0};
    auto s = encode_commit(handle, true);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::application_error, res->code_);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(sql::status::Status::ERR_INVALID_ARGUMENT, error.status_);
    ASSERT_FALSE(error.message_.empty());
}

TEST_F(service_api_test, rollback) {
    std::uint64_t handle{};
    test_begin(handle);
    {
        auto s = encode_rollback(handle);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);
        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
}

TEST_F(service_api_test, error_on_rollback) {
    std::uint64_t handle{0};
    auto s = encode_rollback(handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::application_error, res->code_);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(sql::status::Status::ERR_INVALID_ARGUMENT, error.status_);
    ASSERT_FALSE(error.message_.empty());
}

void service_api_test::test_dispose_prepare(std::uint64_t& handle) {
    auto s = encode_dispose_prepare(handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);
    auto [success, error] = decode_result_only(res->body_);
    ASSERT_TRUE(success);
}

TEST_F(service_api_test, prepare_and_dispose) {
    std::uint64_t handle{};
    test_prepare(handle, "select * from T1");
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
TEST_F(service_api_test, error_on_dispose) {
    std::uint64_t handle{0};
    auto s = encode_dispose_prepare(handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);

    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::application_error, res->code_);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(sql::status::Status::ERR_INVALID_ARGUMENT, error.status_);
    ASSERT_FALSE(error.message_.empty());
}

void service_api_test::test_statement(std::string_view sql, std::uint64_t tx_handle, status exp) {
    auto s = encode_execute_statement(tx_handle, sql);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(exp == status::ok ? response_code::success : response_code::application_error, res->code_);
    EXPECT_TRUE(res->all_released());

    auto [success, error] = decode_result_only(res->body_);
    if(exp == status::ok) {
        ASSERT_TRUE(success);
    } else {
        ASSERT_FALSE(success);
        ASSERT_EQ(api::impl::details::map_status(exp), error.status_);
    }
}
void service_api_test::test_statement(std::string_view sql, std::uint64_t tx_handle) {
    test_statement(sql, tx_handle, status::ok);
}

void service_api_test::test_statement(std::string_view sql) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    test_statement(sql, tx_handle);
    test_commit(tx_handle);
}

void service_api_test::test_query(
    std::string_view sql,
    std::uint64_t tx_handle,
    std::vector<sql::common::AtomType> const& column_types,
    std::vector<bool> const& nullabilities,
    std::vector<mock::basic_record> const& expected,
    std::vector<std::string> const& exp_colnames
) {
    auto s = encode_execute_query(tx_handle, sql);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);
    EXPECT_TRUE(res->all_released());

    {
        auto [name, cols] = decode_execute_query(res->body_head_);
        std::cout << "name : " << name << std::endl;
        ASSERT_EQ(column_types.size(), cols.size());

        for(std::size_t i=0, n=cols.size(); i<n; ++i) {
            EXPECT_EQ(column_types[i], cols[i].type_);
            EXPECT_EQ(nullabilities[i], cols[i].nullable_);
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

void service_api_test::test_query(std::string_view query) {
    std::uint64_t tx_handle{};
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

TEST_F(service_api_test, execute_statement_and_query) {
    test_statement("insert into T0(C0, C1) values (1, 10.0)");
    test_query();
}

TEST_F(service_api_test, execute_prepared_statement_and_query) {
    std::uint64_t tx_handle{};
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
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [success, error] = decode_result_only(res->body_);
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

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->all_released());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::cout << "name : " << name << std::endl;
            ASSERT_EQ(2, cols.size());

            EXPECT_EQ(sql::common::AtomType::INT8, cols[0].type_);
            EXPECT_TRUE(cols[0].nullable_);
            EXPECT_EQ(sql::common::AtomType::FLOAT8, cols[1].type_);
            EXPECT_TRUE(cols[1].nullable_);
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

TEST_F(service_api_test, data_types) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T1(C0, C1, C2, C3, C4) values (:c0, :c1, :c2, :c3, c4)",
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

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [success, error] = decode_result_only(res->body_);
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

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::cout << "name : " << name << std::endl;
            ASSERT_EQ(5, cols.size());

            EXPECT_EQ(sql::common::AtomType::INT4, cols[0].type_);
            EXPECT_TRUE(cols[0].nullable_); //TODO for now all nullable
            EXPECT_EQ(sql::common::AtomType::INT8, cols[1].type_);
            EXPECT_TRUE(cols[1].nullable_);
            EXPECT_EQ(sql::common::AtomType::FLOAT8, cols[2].type_);
            EXPECT_TRUE(cols[2].nullable_);
            EXPECT_EQ(sql::common::AtomType::FLOAT4, cols[3].type_);
            EXPECT_TRUE(cols[3].nullable_);
            EXPECT_EQ(sql::common::AtomType::CHARACTER, cols[4].type_);
            EXPECT_TRUE(cols[4].nullable_);
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

TEST_F(service_api_test, decimals) {
    std::uint64_t tx_handle{};
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

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [success, error] = decode_result_only(res->body_);
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

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            ASSERT_EQ(6, cols.size());

            EXPECT_EQ(sql::common::AtomType::DECIMAL, cols[0].type_);
            EXPECT_TRUE(cols[0].nullable_); //TODO for now all nullable
            EXPECT_EQ(sql::common::AtomType::DECIMAL, cols[1].type_);
            EXPECT_TRUE(cols[1].nullable_);
            EXPECT_EQ(sql::common::AtomType::DECIMAL, cols[2].type_);
            EXPECT_TRUE(cols[2].nullable_);
            EXPECT_EQ(sql::common::AtomType::DECIMAL, cols[3].type_);
            EXPECT_TRUE(cols[3].nullable_);
            EXPECT_EQ(sql::common::AtomType::DECIMAL, cols[4].type_);
            EXPECT_TRUE(cols[4].nullable_);
            EXPECT_EQ(sql::common::AtomType::DECIMAL, cols[5].type_);
            EXPECT_TRUE(cols[5].nullable_);
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
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into TTEMPORALS(K0, K1, K2, K3, K4, C0, C1, C2, C3, C4) values (:p0, :p1, :p2, :p3, p4, :p0, :p1, :p2, :p3, p4)",
        std::pair{"p0"s, sql::common::AtomType::DATE},
        std::pair{"p1"s, sql::common::AtomType::TIME_OF_DAY},
        std::pair{"p2"s, sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE},
        std::pair{"p3"s, sql::common::AtomType::TIME_POINT},
        std::pair{"p4"s, sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE}
    );

    auto d2000_1_1 = date_v{2000, 1, 1};
    auto t12_0_0 = time_of_day_v{12, 0, 0};
    auto tp2000_1_1_12_0_0 = time_point_v{d2000_1_1, t12_0_0};

    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kDateValue, std::any{std::in_place_type<date_v>, d2000_1_1}},
            {"p1"s, ValueCase::kTimeOfDayValue, std::any{std::in_place_type<time_of_day_v>, t12_0_0}},
            {"p2"s, ValueCase::kTimeOfDayWithTimeZoneValue, std::any{std::in_place_type<time_of_day_v>, t12_0_0}},
            {"p3"s, ValueCase::kTimePointValue, std::any{std::in_place_type<time_point_v>, tp2000_1_1_12_0_0}},
            {"p4"s, ValueCase::kTimePointWithTimeZoneValue, std::any{std::in_place_type<time_point_v>, tp2000_1_1_12_0_0}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [success, error] = decode_result_only(res->body_);
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

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            ASSERT_EQ(10, cols.size());

            EXPECT_EQ(sql::common::AtomType::DATE, cols[0].type_);
            EXPECT_TRUE(cols[0].nullable_); //TODO for now all nullable
            EXPECT_EQ(sql::common::AtomType::TIME_OF_DAY, cols[1].type_);
            EXPECT_TRUE(cols[1].nullable_);
            EXPECT_EQ(sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE, cols[2].type_);
            EXPECT_TRUE(cols[2].nullable_);
            EXPECT_EQ(sql::common::AtomType::TIME_POINT, cols[3].type_);
            EXPECT_TRUE(cols[3].nullable_);
            EXPECT_EQ(sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE, cols[4].type_);
            EXPECT_TRUE(cols[4].nullable_);
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
                        d2000_1_1, t12_0_0, t12_0_0, tp2000_1_1_12_0_0, tp2000_1_1_12_0_0,
                        d2000_1_1, t12_0_0, t12_0_0, tp2000_1_1_12_0_0, tp2000_1_1_12_0_0,
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

    req.release_session_handle();
    EXPECT_FALSE(req.has_session_handle());
}

TEST_F(service_api_test, invalid_request) {
    auto req = std::make_shared<tateyama::api::server::mock::test_request>("ABC");
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_NE(response_code::success, res->code_);
}

TEST_F(service_api_test, empty_request) {
    // error returned as "invalid request code"
    auto req = std::make_shared<tateyama::api::server::mock::test_request>("");
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    EXPECT_NE(response_code::success, res->code_);
    EXPECT_NE(response_code::success, res->code_);
}

TEST_F(service_api_test, invalid_stmt_on_execute_prepared_statement_or_query) {
    std::uint64_t tx_handle{};
    std::uint64_t stmt_handle{0};
    {
        test_begin(tx_handle);
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::application_error, res->code_);

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_FALSE(success);
        ASSERT_EQ(sql::status::Status::ERR_INVALID_ARGUMENT, error.status_);
        ASSERT_FALSE(error.message_.empty());
        test_commit(tx_handle, true, error_code::inactive_transaction_exception); // verify tx already aborted
    }
    {
        test_begin(tx_handle);
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::application_error, res->code_);

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_FALSE(success);
        ASSERT_EQ(sql::status::Status::ERR_INVALID_ARGUMENT, error.status_);
        ASSERT_FALSE(error.message_.empty());
        test_rollback(tx_handle); // Even tx has been aborted already, requesting rollback is successful.
        //note that repeating rollback here results in segv because commit or rollback request destroys tx body and tx handle gets dangling
    }
}

void service_api_test::execute_statement_as_query(std::string_view sql) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    auto s = encode_execute_query(tx_handle, "insert into T0(C0, C1) values (1, 10.0)");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::application_error, res->code_);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(sql::status::Status::ERR_ILLEGAL_OPERATION, error.status_);
    ASSERT_FALSE(error.message_.empty());
    test_commit(tx_handle);
}

TEST_F(service_api_test, execute_statement_as_query) {
    execute_statement_as_query("insert into T0(C0, C1) values (1, 10.0)");
    execute_statement_as_query("update T0 set C1=20.0 where C0=1");
}

TEST_F(service_api_test, execute_query_as_statement) {
    test_statement("insert into T0(C0, C1) values (1, 10.0)");
    test_statement("insert into T0(C0, C1) values (2, 20.0)");
    test_statement("insert into T0(C0, C1) values (3, 30.0)");
    test_statement("select * from T0");
}

TEST_F(service_api_test, explain_insert) {
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
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [result, id, version, cols, error] = decode_explain(res->body_);
        ASSERT_FALSE(result.empty());
        EXPECT_EQ(sql_proto_explain_format_id, id);
        EXPECT_EQ(sql_proto_explain_format_version, version);
        ASSERT_TRUE(cols.empty());

        LOG(INFO) << result;
    }
}

TEST_F(service_api_test, explain_query) {
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
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [result, id, version, cols, error] = decode_explain(res->body_);
        ASSERT_FALSE(result.empty());
        EXPECT_EQ(sql_proto_explain_format_id, id);
        EXPECT_EQ(sql_proto_explain_format_version, version);
        EXPECT_EQ(2, cols.size());
        EXPECT_EQ(sql::common::AtomType::INT8, cols[0].type_);
        EXPECT_TRUE(cols[0].nullable_);
        EXPECT_EQ(sql::common::AtomType::FLOAT8, cols[1].type_);
        EXPECT_TRUE(cols[1].nullable_);
        LOG(INFO) << result;
    }
}

TEST_F(service_api_test, explain_error_invalid_handle) {
    std::uint64_t stmt_handle{};
    {
        auto s = encode_explain(stmt_handle, {});
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_NE(response_code::success, res->code_);

        auto [result, id, version, cols, error] = decode_explain(res->body_);
        ASSERT_TRUE(result.empty());
        ASSERT_TRUE(cols.empty());

        ASSERT_EQ(sql::status::Status::ERR_INVALID_ARGUMENT, error.status_);
        ASSERT_FALSE(error.message_.empty());
        LOG(INFO) << error.message_;
    }
}

TEST_F(service_api_test, explain_error_missing_parameter) {
    std::uint64_t stmt_handle{};

    test_prepare(
        stmt_handle,
        "select C0, C1 from T0 where C0 = :c0 and C1 = :c1",
        std::pair{"c0"s, sql::common::AtomType::INT8},
        std::pair{"c1"s, sql::common::AtomType::FLOAT8}
    );
    {
        auto s = encode_explain(stmt_handle, {});
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::application_error, res->code_);

        auto [explained, id, version, cols, error] = decode_explain(res->body_);
        ASSERT_TRUE(explained.empty());
        ASSERT_TRUE(cols.empty());
        ASSERT_EQ(sql::status::Status::ERR_UNRESOLVED_HOST_VARIABLE, error.status_);
        ASSERT_FALSE(error.message_.empty());
    }
}

TEST_F(service_api_test, null_host_variable) {
    std::uint64_t tx_handle{};
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
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [success, error] = decode_result_only(res->body_);
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
    std::uint64_t tx_handle{};
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
    std::uint64_t tx_handle{};
    {
        test_begin(tx_handle, false, true, {"T0"});
        test_statement("insert into T0(C0, C1) values (1, 10.0)", tx_handle);
        test_query(
            "select * from T0 where C0=1",
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
}

TEST_F(service_api_test, execute_ddl) {
    test_statement("create table MYTABLE(C0 bigint primary key, C1 double)");
    test_statement("insert into MYTABLE(C0, C1) values (1, 10.0)");
    test_query("select * from MYTABLE");
}

void service_api_test::test_dump(std::vector<std::string>& files, std::string_view dir, status expected) {
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
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    do {
        std::vector<parameter> parameters{
            {"c0"s, ValueCase::kInt8Value, std::any{std::in_place_type<std::int64_t>, 0}},
            {"c1"s, ValueCase::kFloat8Value, std::any{std::in_place_type<double>, 0.0}},
        };
        auto s = encode_execute_dump(tx_handle, query_handle, parameters, p);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->all_released());
        ASSERT_TRUE(st);
        if (expected != status::ok) {
            ASSERT_EQ(response_code::application_error, res->code_);
            break;
        }
        ASSERT_EQ(response_code::success, res->code_);
        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::cout << "name : " << name << std::endl;
            ASSERT_EQ(1, cols.size());
            EXPECT_EQ(sql::common::AtomType::CHARACTER, cols[0].type_);
            EXPECT_TRUE(cols[0].nullable_);
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

TEST_F(service_api_test, execute_dump_load) {
    std::vector<std::string> files{};
    test_dump(files);
    test_statement("delete from T0");
    std::stringstream ss{};
    for(auto&& s : files) {
        ss << s;
        ss << " ";
    }
    LOG(INFO) << "dump files: " << ss.str();
    test_load(true, status::ok, files[0]);
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 ORDER BY C0", result);
        ASSERT_EQ(10, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(1,10.0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(10,100.0)), result[9]);
    }
}

TEST_F(service_api_test, execute_dump_load_non_tx) {
    std::vector<std::string> files{};
    test_dump(files);
    test_statement("delete from T0");
    std::stringstream ss{};
    for(auto&& s : files) {
        ss << s;
        ss << " ";
    }
    LOG(INFO) << "dump files: " << ss.str();
    test_load(false, status::ok, files[0]);
    {
        using kind = meta::field_type_kind;
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0 ORDER BY C0", result);
        ASSERT_EQ(10, result.size());
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(1,10.0)), result[0]);
        EXPECT_EQ((mock::create_nullable_record<kind::int8, kind::float8>(10,100.0)), result[9]);
    }
}

TEST_F(service_api_test, dump_bad_path) {
    // check if error code is returned correctly
    std::vector<std::string> files{};
    test_dump(files, "/dummy_path", status::err_io_error);
}

TEST_F(service_api_test, dump_error_with_query_result) {
    // test if error in the middle of query processing is handled correctly
    test_statement("insert into T0(C0, C1) values (1, 10.0)");
    test_statement("insert into T0(C0, C1) values (2, 0.0)");
    test_statement("insert into T0(C0, C1) values (3, 30.0)");
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, 1.0/C1 from T0"
    );
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    do {
        auto s = encode_execute_dump(tx_handle, query_handle, {}, std::string{service_api_test::temporary_.path()});

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->all_released());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::application_error, res->code_);
        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::cout << "name : " << name << std::endl;
            ASSERT_EQ(1, cols.size());
            EXPECT_EQ(sql::common::AtomType::CHARACTER, cols[0].type_);
            EXPECT_TRUE(cols[0].nullable_);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());
                LOG(INFO) << v[0];
                boost::filesystem::path p{static_cast<std::string>(v[0].get_value<accessor::text>(0))};
                ASSERT_FALSE(boost::filesystem::exists(p)); // by default, file is deleted on error
                EXPECT_TRUE(ch.all_released());
            }
        }
        {
            auto [success, error] = decode_result_only(res->body_);
            ASSERT_FALSE(success);
            ASSERT_EQ(sql::status::Status::ERR_EXPRESSION_EVALUATION_FAILURE, error.status_);
        }
    } while(0);
    test_commit(tx_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, load_no_file) {
    // no file is specified - success
    std::vector<std::string> files{};
    test_load(true, status::ok);
}

TEST_F(service_api_test, DISABLED_load_no_file_non_tx) {
    // no file is specified - success
    std::vector<std::string> files{};
    test_load(false, status::ok);
}

TEST_F(service_api_test, load_empty_file_name) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    std::vector<std::string> files{};
    test_load(true, status::err_aborted, "");
}

TEST_F(service_api_test, load_empty_file_name_non_tx) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    std::vector<std::string> files{};
    test_load(false, status::err_io_error, "");
}

TEST_F(service_api_test, load_missing_files) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    std::vector<std::string> files{};
    test_load(true, status::err_aborted, "dummy1.parquet", "dummy2.parquet");
}
TEST_F(service_api_test, load_missing_files_non_tx) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    std::vector<std::string> files{};
    test_load(true, status::err_aborted, "dummy1.parquet", "dummy2.parquet");
}

template <class ... Args>
void service_api_test::test_load(bool transactional, status expected, Args...files) {
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0 (C0, C1) values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::INT8},
        std::pair{"p1"s, sql::common::AtomType::FLOAT8}
    );
    std::uint64_t tx_handle{};
    if(transactional) {
        test_begin(tx_handle);
    }
    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kReferenceColumnName, std::any{std::in_place_type<std::string>, "C0"}},
            {"p1"s, ValueCase::kReferenceColumnPosition, std::any{std::in_place_type<std::uint64_t>, 1}},
        };
        auto s = encode_execute_load(tx_handle, stmt_handle, parameters, files...);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->all_released());
        ASSERT_TRUE(st);
        ASSERT_EQ(expected == status::ok ? response_code::success : response_code::application_error, res->code_);
        {
            auto [success, error] = decode_result_only(res->body_);
            if(expected == status::ok) {
                ASSERT_TRUE(success);
                if(transactional) {
                    test_commit(tx_handle);
                }
            } else {
                ASSERT_FALSE(success);
                ASSERT_EQ(api::impl::details::map_status(expected), error.status_);
            }
        }
    }
    test_dispose_prepare(stmt_handle);
}

TEST_F(service_api_test, describe_table) {
    auto s = encode_describe_table("T0");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);

    auto [result, error] = decode_describe_table(res->body_);
    ASSERT_EQ("T0", result.table_name_);
    ASSERT_EQ("", result.schema_name_);
    ASSERT_EQ("", result.database_name_);
    ASSERT_EQ(2, result.columns_.size());
    EXPECT_EQ("C0", result.columns_[0].name_);
    EXPECT_EQ(sql::common::AtomType::INT8, result.columns_[0].atom_type_);
    EXPECT_EQ("C1", result.columns_[1].name_);
    EXPECT_EQ(sql::common::AtomType::FLOAT8, result.columns_[1].atom_type_);
}

TEST_F(service_api_test, describe_table_not_found) {
    auto s = encode_describe_table("DUMMY");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::application_error, res->code_);

    auto [result, error] = decode_describe_table(res->body_);
    ASSERT_EQ(sql::status::ERR_NOT_FOUND, error.status_);
    LOG(INFO) << "error: " << error.message_;
}

TEST_F(service_api_test, describe_pkless_table) {
    // make sure generated pk column is not visible
    execute_statement("create table T (C0 INT)");
    auto s = encode_describe_table("T");
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);

    auto [result, error] = decode_describe_table(res->body_);
    ASSERT_EQ("T", result.table_name_);
    ASSERT_EQ("", result.schema_name_);
    ASSERT_EQ("", result.database_name_);
    ASSERT_EQ(1, result.columns_.size());
    EXPECT_EQ("C0", result.columns_[0].name_);
    EXPECT_EQ(sql::common::AtomType::INT4, result.columns_[0].atom_type_);
}

TEST_F(service_api_test, empty_result_set) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    test_query(
        "select * from T0",
        tx_handle,
        {
            sql::common::AtomType::INT8,
            sql::common::AtomType::FLOAT8
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
        std::uint64_t tx_handle{};
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
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);

    auto result = decode_list_tables(res->body_);
    ASSERT_TRUE(contains(result, "TT0"));
    ASSERT_TRUE(contains(result, "TT1"));
    ASSERT_FALSE(contains(result, "II"));
}

TEST_F(service_api_test, get_search_path) {
    auto s = encode_get_search_path();
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);

    auto result = decode_get_search_path(res->body_);
    ASSERT_EQ(0, result.size());
}

TEST_F(service_api_test, modifies_definitions) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle, false, true, {}, "modifies_definitions", true);
    test_statement("CREATE TABLE TT(C0 INT)", tx_handle);
    test_commit(tx_handle);
}

TEST_F(service_api_test, get_error_info) {
    // verify get error info is not affected by err_inactive_transaction (request failure, not transaction failure)
    test_statement("CREATE TABLE TT(C0 INT NOT NULL PRIMARY KEY)");
    test_statement("INSERT INTO TT VALUES (0)");
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    test_statement("INSERT INTO TT VALUES (0)", tx_handle, status::err_unique_constraint_violation);
    test_statement("INSERT INTO TT VALUES (1)", tx_handle, status::err_inactive_transaction);
    test_statement("INSERT INTO TT VALUES (2)", tx_handle, status::err_inactive_transaction);
    test_get_error_info(tx_handle, error_code::unique_constraint_violation_exception);
    test_dispose_transaction(tx_handle);
}

TEST_F(service_api_test, dispose_transaction_invalid_handle) {
    test_dispose_transaction(1);  // disposing invalid handle is no-op
}

TEST_F(service_api_test, dispose_transaction_missing_handle) {
    // protobuf treats 0 as if not handle is specified
    // this case is handled as an error because sending 0 is usage error anyway
    test_dispose_transaction(0, status::err_invalid_argument);
}

TEST_F(service_api_test, dispose_transaction) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory cannot spwan multiple transactions";
    }
    std::uint64_t tx_handle0{};
    test_begin(tx_handle0);
    std::uint64_t tx_handle1{};
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
        std::uint64_t tx_handle{};
        test_begin(tx_handle);
        test_statement("INSERT INTO TT VALUES (0)", tx_handle, status::err_unique_constraint_violation);

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
        std::uint64_t tx_handle{};
        test_begin(tx_handle);
        test_statement("INSERT INTO TT VALUES (1)", tx_handle, status::ok);
        test_commit(tx_handle);

        EXPECT_EQ(0, get_impl(*db_).transaction_count());
        test_dispose_transaction(tx_handle); // this is no-op
    }
}

void service_api_test::test_dispose_transaction(
    std::uint64_t handle,
    status expected
) {
    auto s = encode_dispose_transaction(handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();

    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(expected == status::ok ? response_code::success : response_code::application_error, res->code_);

    auto [success, error] = decode_result_only(res->body_);
    if(expected == status::ok) {
        ASSERT_TRUE(success);
    } else {
        ASSERT_FALSE(success);
        ASSERT_EQ(api::impl::details::map_status(expected), error.status_);
    }
}

TEST_F(service_api_test, get_error_info_on_compile_error) {
    // verify get error info with compile error
    test_statement("CREATE TABLE TT(C0 INT NOT NULL PRIMARY KEY)");
    test_statement("INSERT INTO TT VALUES (0)");
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    test_statement("INSERT INTO dummy VALUES (0)", tx_handle, status::err_compiler_error);
    test_statement("INSERT INTO TT VALUES (1)", tx_handle, status::err_inactive_transaction);
    test_get_error_info(tx_handle, error_code::symbol_analyze_exception);
    test_dispose_transaction(tx_handle);
}

}
