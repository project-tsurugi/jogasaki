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

#include <jogasaki/kvs/id.h>
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
#include <tateyama/api/endpoint/service.h>
#include <tateyama/api/environment.h>
#include <tateyama/api/server/service.h>
#include "api_test_base.h"
#include <jogasaki/utils/msgbuf_utils.h>

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
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
namespace sql = jogasaki::proto::sql;
using ValueCase = sql::request::Parameter::ValueCase;

using takatori::util::unsafe_downcast;
using takatori::util::maybe_shared_ptr;
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
        cfg->single_thread(false);
        set_dbpath(*cfg);

        db_ = std::shared_ptr{jogasaki::api::create_database(cfg)};
        auto c = std::make_shared<tateyama::api::configuration::whole>("");
        service_ = std::make_shared<jogasaki::api::impl::service>(c, db_.get());
        db_->start();

        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());

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
        std::vector<std::string> const& write_preserves = {}
    );
    void test_commit(std::uint64_t& handle);
    void test_statement(std::string_view sql);
    void test_statement(std::string_view sql, std::uint64_t tx_handle);
    void test_query(std::string_view query = "select * from T0");

    void test_query(
        std::string_view sql,
        std::uint64_t tx_handle,
        std::vector<sql::common::AtomType> const& column_types,
        std::vector<bool> const& nullabilities,
        std::vector<mock::basic_record> const& expected,
        std::vector<std::string> const& exp_colnames
    );

    bool wait_completion(tateyama::api::server::mock::test_response& res, std::size_t timeout_ms = 2000) {
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
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::success, res->code_);
        handle = decode_prepare(res->body_);
    }
    void test_dispose_prepare(std::uint64_t& handle);


    void test_dump(std::vector<std::string>& files, std::string_view dir = "", status expected = status::ok);

    template <class ... Args>
    void test_load(status expected, Args ... args);

    std::shared_ptr<jogasaki::api::impl::service> service_{};  //NOLINT
    test::temporary_folder temporary_{};


};


void service_api_test::test_begin(std::uint64_t& handle,
    bool readonly,
    bool is_long,
    std::vector<std::string> const& write_preserves
) {
    auto s = encode_begin(readonly, is_long, write_preserves);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);
    handle = decode_begin(res->body_);
}

void service_api_test::test_commit(std::uint64_t& handle) {
    auto s = encode_commit(handle);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->completed());
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
    auto s = encode_commit(handle);
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

TEST_F(service_api_test, disconnect) {
    std::uint64_t handle{};
    {
        auto s = encode_disconnect();
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

void service_api_test::test_statement(std::string_view sql, std::uint64_t tx_handle) {
    auto s = encode_execute_statement(tx_handle, sql);
    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(wait_completion(*res));
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::success, res->code_);
    EXPECT_TRUE(res->all_released());

    auto [success, error] = decode_result_only(res->body_);
    ASSERT_TRUE(success);
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
    EXPECT_TRUE(wait_completion(*res));
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
            ASSERT_EQ(1, ch.buffers_.size());
            ASSERT_TRUE(ch.buffers_[0]);
            auto& buf = *ch.buffers_[0];
            auto m = create_record_meta(cols);
            auto v = deserialize_msg(buf.view(), m);
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
        EXPECT_TRUE(wait_completion(*res));
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
        EXPECT_TRUE(wait_completion(*res));
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
        EXPECT_TRUE(wait_completion(*res));
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
        EXPECT_TRUE(wait_completion(*res));
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
    test_begin(tx_handle);
    std::uint64_t stmt_handle{0};
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::application_error, res->code_);

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_FALSE(success);
        ASSERT_EQ(sql::status::Status::ERR_INVALID_ARGUMENT, error.status_);
        ASSERT_FALSE(error.message_.empty());
    }
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, stmt_handle, parameters);
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::application_error, res->code_);

        auto [success, error] = decode_result_only(res->body_);
        ASSERT_FALSE(success);
        ASSERT_EQ(sql::status::Status::ERR_INVALID_ARGUMENT, error.status_);
        ASSERT_FALSE(error.message_.empty());
    }
    test_commit(tx_handle);
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
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
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
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
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
        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_NE(response_code::success, res->code_);

        auto [result, error] = decode_explain(res->body_);
        ASSERT_TRUE(result.empty());
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
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);
        ASSERT_EQ(response_code::application_error, res->code_);

        auto [explained, error] = decode_explain(res->body_);
        ASSERT_TRUE(explained.empty());
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
        EXPECT_TRUE(wait_completion(*res));
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
        test_begin(tx_handle, false, true, {"T0", "T1"});
        test_commit(tx_handle);
    }
    {
        test_begin(tx_handle, true, true, {});
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
        EXPECT_TRUE(wait_completion(*res));
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
                ASSERT_LT(0, ch.buffers_.size());
                ASSERT_TRUE(ch.buffers_[0]);
                auto& buf = *ch.buffers_[0];
                ASSERT_LT(0, buf.view().size());
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(buf.view(), m);
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
    test_load(status::ok, files[0]);
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
        EXPECT_TRUE(wait_completion(*res));
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
                ASSERT_LT(0, ch.buffers_.size());
                ASSERT_TRUE(ch.buffers_[0]);
                auto& buf = *ch.buffers_[0];
                ASSERT_LT(0, buf.view().size());
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(buf.view(), m);
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
    test_load(status::ok);
}

TEST_F(service_api_test, load_empty_file_name) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    std::vector<std::string> files{};
    test_load(status::err_aborted, "");
}

TEST_F(service_api_test, load_missing_files) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    std::vector<std::string> files{};
    test_load(status::err_aborted, "dummy1.parquet", "dummy2.parquet");
}

template <class ... Args>
void service_api_test::test_load(status expected, Args...files) {
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0 (C0, C1) values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::INT8},
        std::pair{"p1"s, sql::common::AtomType::FLOAT8}
    );
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kReferenceColumnName, std::any{std::in_place_type<std::string>, "C0"}},
            {"p1"s, ValueCase::kReferenceColumnPosition, std::any{std::in_place_type<std::uint64_t>, 1}},
        };
        auto s = encode_execute_load(tx_handle, stmt_handle, parameters, files...);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(wait_completion(*res));
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->all_released());
        ASSERT_TRUE(st);
        ASSERT_EQ(expected == status::ok ? response_code::success : response_code::application_error, res->code_);
        {
            auto [success, error] = decode_result_only(res->body_);
            if(expected == status::ok) {
                ASSERT_TRUE(success);
                test_commit(tx_handle);
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
    EXPECT_TRUE(wait_completion(*res));
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
    EXPECT_TRUE(wait_completion(*res));
    EXPECT_TRUE(res->completed());
    ASSERT_TRUE(st);
    ASSERT_EQ(response_code::application_error, res->code_);

    auto [result, error] = decode_describe_table(res->body_);
    ASSERT_EQ(sql::status::ERR_NOT_FOUND, error.status_);
    LOG(INFO) << "error: " << error.message_;
}
}
