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
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/utils/binary_printer.h>

#include <tateyama/api/endpoint/mock/endpoint_impls.h>
#include <tateyama/api/endpoint/service.h>
#include "api_test_base.h"

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "schema.pb.h"

namespace jogasaki::api {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace tateyama::api::endpoint;

using takatori::util::unsafe_downcast;
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
        service_ = tateyama::api::endpoint::create_service(*db_);
    }

    void TearDown() override {
        db_teardown();
    }
    void test_begin(std::uint64_t& handle);
    void test_commit(std::uint64_t& handle);

    template <class ...Args>
    void test_prepare(std::uint64_t& handle, std::string sql, Args...args) {
        std::vector<std::pair<std::string, common::DataType>> place_holders{args...};
        ::request::Request r{};
        auto* p = r.mutable_prepare();
        p->mutable_sql()->assign(sql);
        if (! place_holders.empty()) {
            auto vars = p->mutable_host_variables();
            for(auto&& [n, t] : place_holders) {
                auto* v = vars->add_variables();
                v->set_name(n);
                v->set_type(t);
            }
        }
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_prepare());
        auto& prep = resp.prepare();
        ASSERT_TRUE(prep.has_prepared_statement_handle());
        auto& stmt = prep.prepared_statement_handle();
        handle = stmt.handle();
    }
    void test_dispose_prepare(std::uint64_t& handle);

    std::unique_ptr<tateyama::api::endpoint::service> service_{};
};

using namespace std::string_view_literals;

std::string serialize(::request::Request& r) {
    std::string s{};
    if (!r.SerializeToString(&s)) {
        std::abort();
    }
    std::cout << " DebugString : " << r.DebugString() << std::endl;
    std::cout << " Binary data : " << utils::binary_printer{s.data(), s.size()} << std::endl;
    return s;
}

void deserialize(std::string_view s, ::response::Response& res) {
    if (!res.ParseFromString(std::string(s))) {
        std::abort();
    }
    std::cout << " Binary data : " << utils::binary_printer{s.data(), s.size()} << std::endl;
    std::cout << " DebugString : " << res.DebugString() << std::endl;
}

void service_api_test::test_begin(std::uint64_t& handle) {
    ::request::Request r{};
    r.mutable_begin()->set_read_only(false);
    r.mutable_session_handle()->set_handle(1);
    auto s = serialize(r);

    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

    auto st = (*service_)(req, res);
    // TODO the operation can be asynchronous. Wait until response becomes ready.
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::success, res->code_);

    ::response::Response resp{};
    deserialize(res->body_, resp);
    ASSERT_TRUE(resp.has_begin());
    auto& begin = resp.begin();
    ASSERT_TRUE(begin.has_transaction_handle());
    auto& tx = begin.transaction_handle();
    handle = tx.handle();
}

void service_api_test::test_commit(std::uint64_t& handle) {
    ::request::Request r{};
    r.mutable_commit()->mutable_transaction_handle()->set_handle(handle);
    auto s = serialize(r);

    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

    auto st = (*service_)(req, res);
    // TODO the operation can be asynchronous. Wait until response becomes ready.
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::success, res->code_);
}

TEST_F(service_api_test, begin_and_commit) {
    std::uint64_t handle{};
    test_begin(handle);
    test_commit(handle);
}

TEST_F(service_api_test, rollback) {
    std::uint64_t handle{};
    test_begin(handle);
    {
        ::request::Request r{};
        r.mutable_rollback()->mutable_transaction_handle()->set_handle(handle);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
    }
}

void service_api_test::test_dispose_prepare(std::uint64_t& handle) {
    ::request::Request r{};
    r.mutable_dispose_prepared_statement()->mutable_prepared_statement_handle()->set_handle(handle);
    auto s = serialize(r);

    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

    auto st = (*service_)(req, res);
    // TODO the operation can be asynchronous. Wait until response becomes ready.
    ASSERT_EQ(tateyama::status::ok, st);
    ASSERT_EQ(response_code::success, res->code_);
}

TEST_F(service_api_test, prepare_and_dispose) {
    std::uint64_t handle{};
    test_prepare(handle, "select * from T1");
    test_dispose_prepare(handle);
}

TEST_F(service_api_test, disconnect) {
    std::uint64_t handle{};
    {
        ::request::Request r{};
        r.mutable_disconnect();
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_result_only());
        auto& ro = resp.result_only();
        ASSERT_TRUE(ro.has_success());
    }
}

TEST_F(service_api_test, execute_statement_and_query) {
    std::uint64_t tx_handle{};
    test_begin(tx_handle);
    {
        ::request::Request r{};
        auto* stmt = r.mutable_execute_statement();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_sql()->assign("insert into T0(C0, C1) values (1, 1.0)");
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_result_only());
        auto& ro = resp.result_only();
        ASSERT_TRUE(ro.has_success());
    }
    test_commit(tx_handle);
    test_begin(tx_handle);
    {
        ::request::Request r{};
        auto* stmt = r.mutable_execute_query();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_sql()->assign("select * from T0");
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_execute_query());
        auto& eq = resp.execute_query();

        ASSERT_FALSE(eq.has_error());
        ASSERT_TRUE(eq.has_result_set_info());
        auto& rsinfo = eq.result_set_info();
        std::cout << "name : " << rsinfo.name() << std::endl;
        ASSERT_TRUE(rsinfo.has_record_meta());
        auto meta = rsinfo.record_meta();
        ASSERT_EQ(2, meta.columns_size());
        meta.columns(0).type();

        EXPECT_EQ(::common::DataType::INT8, meta.columns(0).type());
        EXPECT_TRUE(meta.columns(0).nullable());
        EXPECT_EQ(::common::DataType::FLOAT8, meta.columns(1).type());
        EXPECT_TRUE(meta.columns(1).nullable());
    }
    test_commit(tx_handle);
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
        ::request::Request r{};
        auto* stmt = r.mutable_execute_prepared_statement();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_prepared_statement_handle()->set_handle(stmt_handle);
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_result_only());
        auto& ro = resp.result_only();
        ASSERT_TRUE(ro.has_success());
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select * from T0 where c0 = :c0 and c1 = :c1",
        std::pair{"c0"s, ::common::DataType::INT8},
        std::pair{"c1"s, ::common::DataType::FLOAT8}
    );
    test_begin(tx_handle);
    {
        ::request::Request r{};
        auto* stmt = r.mutable_execute_prepared_query();
        stmt->mutable_transaction_handle()->set_handle(tx_handle);
        stmt->mutable_prepared_statement_handle()->set_handle(query_handle);
        auto* params = stmt->mutable_parameters();
        auto* c0 = params->add_parameters();
        c0->set_name("c0");
        c0->set_l_value(0);
        auto* c1 = params->add_parameters();
        c1->set_name("c1");
        c1->set_d_value(0.0);
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);
        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto st = (*service_)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);

        ::response::Response resp{};
        deserialize(res->body_, resp);
        ASSERT_TRUE(resp.has_execute_query());
        auto& eq = resp.execute_query();

        ASSERT_FALSE(eq.has_error());
        ASSERT_TRUE(eq.has_result_set_info());
        auto& rsinfo = eq.result_set_info();
        std::cout << "name : " << rsinfo.name() << std::endl;
        ASSERT_TRUE(rsinfo.has_record_meta());
        auto meta = rsinfo.record_meta();
        ASSERT_EQ(2, meta.columns_size());
        meta.columns(0).type();

        EXPECT_EQ(::common::DataType::INT8, meta.columns(0).type());
        EXPECT_TRUE(meta.columns(0).nullable());
        EXPECT_EQ(::common::DataType::FLOAT8, meta.columns(1).type());
        EXPECT_TRUE(meta.columns(1).nullable());
    }
    test_commit(tx_handle);
    test_dispose_prepare(stmt_handle);
    test_dispose_prepare(query_handle);
}

}
