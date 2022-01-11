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

#include <msgpack.hpp>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/utils/command_utils.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/latch.h>

#include <tateyama/api/endpoint/mock/endpoint_impls.h>
#include <tateyama/api/endpoint/service.h>
#include <tateyama/api/registry.h>
#include <tateyama/api/environment.h>
#include <tateyama/api/server/service.h>
#include "api_test_base.h"
#include <jogasaki/utils/msgbuf_utils.h>

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "schema.pb.h"
#include "status.pb.h"

namespace jogasaki::api {

using namespace std::chrono_literals;
using namespace std::string_view_literals;
using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::utils;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace tateyama::api::endpoint;

using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;
std::string serialize(::request::Request& r);
void deserialize(std::string_view s, ::response::Response& res);

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
        cfg->single_thread(true);
        db_setup(cfg);
        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());

        environment_ = std::make_unique<tateyama::api::environment>();
        auto app = tateyama::api::registry<tateyama::api::server::service>::create("jogasaki");
        environment_->add_application(app);
        app->initialize(*environment_, db_.get());
        service_ = tateyama::api::endpoint::create_service(*environment_);
        environment_->endpoint_service(service_);
        auto endpoint = tateyama::api::registry<tateyama::api::endpoint::provider>::create("mock");
        environment_->add_endpoint(endpoint);
        endpoint->initialize(*environment_, {});

        utils::utils_raise_exception_on_error = true;
    }

    void TearDown() override {
        environment_->endpoints()[0]->shutdown();
        environment_->applications()[0]->shutdown();
        db_teardown();
    }
    void test_begin(std::uint64_t& handle);
    void test_commit(std::uint64_t& handle);
    void test_statement(std::string_view sql);
    void test_query();

    bool wait_completion(tateyama::api::endpoint::mock::test_response& res, std::size_t timeout_ms = 2000) {
        auto begin = std::chrono::steady_clock::now();
        while(! res.completed()) {
            using namespace std::chrono_literals;
            std::this_thread::sleep_for(10ms);
            auto cur = std::chrono::steady_clock::now();
            if (std::chrono::duration_cast<std::chrono::milliseconds>(cur - begin).count() > timeout_ms) {
                return false;
            }
        }
        return true;
    }

    template <class ...Args>
    void test_prepare(std::uint64_t& handle, std::string sql, Args...args) {
        auto s = encode_prepare(sql, args...);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
        handle = decode_prepare(res->body_);
    }
    void test_dispose_prepare(std::uint64_t& handle);

    std::shared_ptr<tateyama::api::endpoint::service> service_{};  //NOLINT
    std::unique_ptr<tateyama::api::environment> environment_{};  //NOLINT
};


void service_api_test::test_begin(std::uint64_t& handle) {
    auto s = encode_begin(false);
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::success, res->code_);
    handle = decode_begin(res->body_);
}

void service_api_test::test_commit(std::uint64_t& handle) {
    auto s = encode_commit(handle);
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
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
    auto s = encode_commit(handle);
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::application_error, res->code_);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(::status::Status::ERR_INVALID_ARGUMENT, error.status_);
    ASSERT_FALSE(error.message_.empty());
}

TEST_F(service_api_test, rollback) {
    std::uint64_t handle{};
    test_begin(handle);
    {
        auto s = encode_rollback(handle);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
}

TEST_F(service_api_test, error_on_rollback) {
    std::uint64_t handle{0};
    auto s = encode_rollback(handle);
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::application_error, res->code_);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(::status::Status::ERR_INVALID_ARGUMENT, error.status_);
    ASSERT_FALSE(error.message_.empty());
}

void service_api_test::test_dispose_prepare(std::uint64_t& handle) {
    auto s = encode_dispose_prepare(handle);
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::success, res->code_);
    auto [success, error] = decode_result_only(res->body_);
    ASSERT_TRUE(success);
}

TEST_F(service_api_test, prepare_and_dispose) {
    std::uint64_t handle{};
    test_prepare(handle, "select * from T1");
    test_dispose_prepare(handle);
}

TEST_F(service_api_test, error_on_dispose) {
    std::uint64_t handle{0};
    auto s = encode_dispose_prepare(handle);
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);

    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::application_error, res->code_);

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_FALSE(success);
    ASSERT_EQ(::status::Status::ERR_INVALID_ARGUMENT, error.status_);
    ASSERT_FALSE(error.message_.empty());
}

TEST_F(service_api_test, disconnect) {
    std::uint64_t handle{};
    {
        auto s = encode_disconnect();
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
}

void service_api_test::test_statement(std::string_view sql) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    {
        auto s = encode_execute_statement(tx_handle, sql);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
        EXPECT_TRUE(res->all_released());

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
}

void service_api_test::test_query() {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    auto s = encode_execute_query(tx_handle, "select * from T0");
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(wait_completion(*res));
    EXPECT_TRUE(res->completed());
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::success, res->code_);
    EXPECT_TRUE(res->all_released());

    {
        auto [name, cols] = decode_execute_query(res->body_head_);
        std::cout << "name : " << name << std::endl;
        ASSERT_EQ(2, cols.size());

        EXPECT_EQ(::common::DataType::INT8, cols[0].type_);
        EXPECT_TRUE(cols[0].nullable_);
        EXPECT_EQ(::common::DataType::FLOAT8, cols[1].type_);
        EXPECT_TRUE(cols[1].nullable_);
        {
            ASSERT_TRUE(res->channel_);
            auto& ch = *res->channel_;
            ASSERT_EQ(1, ch.buffers_.size());
            ASSERT_TRUE(ch.buffers_[0]);
            auto& buf = *ch.buffers_[0];
            auto m = create_record_meta(cols);
            auto v = deserialize_msg(buf.view(), m);
            ASSERT_EQ(1, v.size());
            EXPECT_EQ((mock::create_nullable_record<meta::field_type_kind::int8, meta::field_type_kind::float8>(1, 10.0)), v[0]);
            EXPECT_TRUE(ch.all_released());
        }
    }
    {
        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
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
        std::pair{"c0"s, ::common::DataType::INT8},
        std::pair{"c1"s, ::common::DataType::FLOAT8}
    );
    {
        std::vector<parameter> parameters{
            {"c0"s, ::common::DataType::INT8, std::any{std::in_place_type<std::int64_t>, 1}},
            {"c1"s, ::common::DataType::FLOAT8, std::any{std::in_place_type<double>, 10.0}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, C1 from T0 where C0 = :c0 and C1 = :c1",
        std::pair{"c0"s, ::common::DataType::INT8},
        std::pair{"c1"s, ::common::DataType::FLOAT8}
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{
            {"c0"s, ::common::DataType::INT8, std::any{std::in_place_type<std::int64_t>, 1}},
            {"c1"s, ::common::DataType::FLOAT8, std::any{std::in_place_type<double>, 10.0}},
        };
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->all_released());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::cout << "name : " << name << std::endl;
            ASSERT_EQ(2, cols.size());

            EXPECT_EQ(::common::DataType::INT8, cols[0].type_);
            EXPECT_TRUE(cols[0].nullable_);
            EXPECT_EQ(::common::DataType::FLOAT8, cols[1].type_);
            EXPECT_TRUE(cols[1].nullable_);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                ASSERT_EQ(1, ch.buffers_.size());
                ASSERT_TRUE(ch.buffers_[0]);
                auto& buf = *ch.buffers_[0];
                ASSERT_LT(0, buf.view().size());
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(buf.view(), m);
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
    test_statement("insert into T0(C0, C1) values (1, 10.0)");

    static constexpr std::size_t num_thread = 10;
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

TEST_F(service_api_test, msgpack1) {
    // verify msgpack behavior
    std::stringstream ss;
    {
        msgpack::pack(ss, msgpack::type::nil_t()); // nil can be put without specifying the type
        std::int32_t i32{1};
        msgpack::pack(ss, i32);
        i32 = 100000;
        msgpack::pack(ss, i32);
        std::int64_t i64{2};
        msgpack::pack(ss, i64);
        float f4{10.0};
        msgpack::pack(ss, f4);
        float f8{11.0};
        msgpack::pack(ss, f8);
        msgpack::pack(ss, "ABC"sv);
    }

    std::string str{ss.str()};
    std::size_t offset{};
    std::int32_t i32{};
    std::int64_t i64{};
    EXPECT_FALSE(extract(str, i32, offset));  // nil can be read as any type
    ASSERT_EQ(1, offset);
    EXPECT_TRUE(extract(str, i32, offset));
    EXPECT_EQ(1, i32);
    ASSERT_EQ(2, offset);
    EXPECT_TRUE(extract(str, i32, offset));
    EXPECT_EQ(100000, i32);
    ASSERT_EQ(7, offset);
    EXPECT_TRUE(extract(str, i64, offset));
    EXPECT_EQ(2, i64);
    ASSERT_EQ(8, offset);
}

TEST_F(service_api_test, data_types) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T1(C0, C1, C2, C3, C4) values (:c0, :c1, :c2, :c3, c4)",
        std::pair{"c0"s, ::common::DataType::INT4},
        std::pair{"c1"s, ::common::DataType::INT8},
        std::pair{"c2"s, ::common::DataType::FLOAT8},
        std::pair{"c3"s, ::common::DataType::FLOAT4},
        std::pair{"c4"s, ::common::DataType::CHARACTER}
    );
    for(std::size_t i=0; i < 3; ++i) {
        std::vector<parameter> parameters{
            {"c0"s, ::common::DataType::INT4, std::any{std::in_place_type<std::int32_t>, i}},
            {"c1"s, ::common::DataType::INT8, std::any{std::in_place_type<std::int64_t>, i}},
            {"c2"s, ::common::DataType::FLOAT8, std::any{std::in_place_type<double>, i}},
            {"c3"s, ::common::DataType::FLOAT4, std::any{std::in_place_type<float>, i}},
            {"c4"s, ::common::DataType::CHARACTER, std::any{std::in_place_type<std::string>, std::to_string(i)}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, C1, C2, C3, C4 from T1 where C1 > :c1 and C2 > :c2 and C4 > :c4 order by C0",
        std::pair{"c1"s, ::common::DataType::INT8},
        std::pair{"c2"s, ::common::DataType::FLOAT8},
        std::pair{"c4"s, ::common::DataType::CHARACTER}
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{
            {"c1"s, ::common::DataType::INT8, std::any{std::in_place_type<std::int64_t>, 0}},
            {"c2"s, ::common::DataType::FLOAT8, std::any{std::in_place_type<double>, 0.0}},
            {"c4"s, ::common::DataType::CHARACTER, std::any{std::in_place_type<std::string>, "0"}},
        };
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::cout << "name : " << name << std::endl;
            ASSERT_EQ(5, cols.size());

            EXPECT_EQ(::common::DataType::INT4, cols[0].type_);
            EXPECT_TRUE(cols[0].nullable_); //TODO for now all nullable
            EXPECT_EQ(::common::DataType::INT8, cols[1].type_);
            EXPECT_TRUE(cols[1].nullable_);
            EXPECT_EQ(::common::DataType::FLOAT8, cols[2].type_);
            EXPECT_TRUE(cols[2].nullable_);
            EXPECT_EQ(::common::DataType::FLOAT4, cols[3].type_);
            EXPECT_TRUE(cols[3].nullable_);
            EXPECT_EQ(::common::DataType::CHARACTER, cols[4].type_);
            EXPECT_TRUE(cols[4].nullable_);
            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                ASSERT_EQ(1, ch.buffers_.size());
                ASSERT_TRUE(ch.buffers_[0]);
                auto& buf = *ch.buffers_[0];
                ASSERT_LT(0, buf.view().size());
                std::cout << "buf size : " << buf.view().size() << std::endl;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(buf.view(), m);
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

TEST_F(service_api_test, protobuf1) {
    // verify protobuf behavior
    using namespace std::string_view_literals;
    std::stringstream ss;
    ::request::Request req{};
    EXPECT_FALSE(req.has_begin());
    EXPECT_FALSE(req.has_session_handle());
    auto& h = req.session_handle();
    EXPECT_EQ(0, h.handle());  // default object has zero handle, that means empty
    auto* session = req.mutable_session_handle();
    EXPECT_TRUE(req.has_session_handle());
    req.clear_session_handle();
    EXPECT_FALSE(req.has_session_handle());

    ::common::Session s;
    req.set_allocated_session_handle(&s);
    EXPECT_TRUE(req.has_session_handle());
    req.release_session_handle();
    EXPECT_FALSE(req.has_session_handle());
}

TEST_F(service_api_test, invalid_request) {
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>("ABC");
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    EXPECT_EQ(tateyama::status::ok, st);
    EXPECT_NE(response_code::success, res->code_);
}

TEST_F(service_api_test, empty_request) {
    // error returned as "invalid request code"
    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>("");
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    EXPECT_EQ(tateyama::status::ok, st);
    EXPECT_NE(response_code::success, res->code_);
}

TEST_F(service_api_test, invalid_stmt_on_execute_prepared_statement_or_query) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{0};
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::application_error, res->code_);

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_FALSE(success);
        ASSERT_EQ(::status::Status::ERR_INVALID_ARGUMENT, error.status_);
        ASSERT_FALSE(error.message_.empty());
    }
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::application_error, res->code_);

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_FALSE(success);
        ASSERT_EQ(::status::Status::ERR_INVALID_ARGUMENT, error.status_);
        ASSERT_FALSE(error.message_.empty());
    }
    test_commit(tx_handle);
}

TEST_F(service_api_test, explain_insert) {
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0(C0, C1) values (:c0, :c1)",
        std::pair{"c0"s, ::common::DataType::INT8},
        std::pair{"c1"s, ::common::DataType::FLOAT8}
    );
    {
        std::vector<parameter> parameters{
            {"c0"s, ::common::DataType::INT8, std::any{std::in_place_type<std::int64_t>, 1}},
            {"c1"s, ::common::DataType::FLOAT8, std::any{std::in_place_type<double>, 10.0}},
        };
        auto s = encode_explain(stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [result, error] = decode_explain(res->body_);
        ASSERT_FALSE(result.empty());
        LOG(INFO) << result;
    }
}

TEST_F(service_api_test, explain_query) {
    std::uint64_t stmt_handle{};

    test_prepare(
        stmt_handle,
        "select C0, C1 from T0 where C0 = :c0 and C1 = :c1",
        std::pair{"c0"s, ::common::DataType::INT8},
        std::pair{"c1"s, ::common::DataType::FLOAT8}
    );
    {
        std::vector<parameter> parameters{
            {"c0"s, ::common::DataType::INT8, std::any{std::in_place_type<std::int64_t>, 1}},
            {"c1"s, ::common::DataType::FLOAT8, std::any{std::in_place_type<double>, 10.0}},
        };
        auto s = encode_explain(stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        auto [result, error] = decode_explain(res->body_);
        ASSERT_FALSE(result.empty());
        LOG(INFO) << result;
    }
}

TEST_F(service_api_test, explain_error_invalid_handle) {
    std::uint64_t stmt_handle{};
    {
        auto s = encode_explain(stmt_handle, {});
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_NE(response_code::success, res->code_);

        auto [result, error] = decode_explain(res->body_);
        ASSERT_TRUE(result.empty());
        ASSERT_EQ(::status::Status::ERR_INVALID_ARGUMENT, error.status_);
        ASSERT_FALSE(error.message_.empty());
        LOG(INFO) << error.message_;
    }
}

TEST_F(service_api_test, explain_error_missing_parameter) {
    std::uint64_t stmt_handle{};

    test_prepare(
        stmt_handle,
        "select C0, C1 from T0 where C0 = :c0 and C1 = :c1",
        std::pair{"c0"s, ::common::DataType::INT8},
        std::pair{"c1"s, ::common::DataType::FLOAT8}
    );
    {
        auto s = encode_explain(stmt_handle, {});
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::application_error, res->code_);

        auto [explained, error] = decode_explain(res->body_);
        ASSERT_TRUE(explained.empty());
        ASSERT_EQ(::status::Status::ERR_UNRESOLVED_HOST_VARIABLE, error.status_);
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
        std::pair{"c0"s, ::common::DataType::INT8},
        std::pair{"c1"s, ::common::DataType::FLOAT8}
    );
    {
        std::vector<parameter> parameters{
            {"c0"s, ::common::DataType::INT8, std::any{std::in_place_type<std::int64_t>, 1}},
            {"c1"s, ::common::DataType::FLOAT8, std::any{}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_EQ(tateyama::status::ok, st);
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
}
