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
    }

    void TearDown() override {
        db_teardown();
    }
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

TEST_F(service_api_test, begin_and_commit) {
    std::uint64_t handle{};
    {
        ::request::Request r{};
        r.mutable_begin()->set_read_only(true);
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto svc = tateyama::api::endpoint::create_service(*db_);

        auto st = (*svc)(req, res);
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
    {
        ::request::Request r{};
        r.mutable_commit()->mutable_transaction_handle()->set_handle(handle);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto svc = tateyama::api::endpoint::create_service(*db_);

        auto st = (*svc)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
    }
}

TEST_F(service_api_test, rollback) {
    std::uint64_t handle{};
    {
        ::request::Request r{};
        r.mutable_begin()->set_read_only(true);
        r.mutable_session_handle()->set_handle(1);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto svc = tateyama::api::endpoint::create_service(*db_);

        auto st = (*svc)(req, res);
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
    {
        ::request::Request r{};
        r.mutable_rollback()->mutable_transaction_handle()->set_handle(handle);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto svc = tateyama::api::endpoint::create_service(*db_);

        auto st = (*svc)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
    }
}

TEST_F(service_api_test, prepare_and_dispose) {
    std::uint64_t handle{};
    {
        ::request::Request r{};
        r.mutable_prepare()->mutable_sql()->assign("select * from T1");
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

        auto svc = tateyama::api::endpoint::create_service(*db_);

        auto st = (*svc)(req, res);
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
    {
        ::request::Request r{};
        r.mutable_dispose_prepared_statement()->mutable_prepared_statement_handle()->set_handle(handle);
        auto s = serialize(r);

        auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
        auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();
        auto svc = tateyama::api::endpoint::create_service(*db_);

        auto st = (*svc)(req, res);
        // TODO the operation can be asynchronous. Wait until response becomes ready.
        ASSERT_EQ(tateyama::status::ok, st);
        ASSERT_EQ(response_code::success, res->code_);
    }
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

        auto svc = tateyama::api::endpoint::create_service(*db_);

        auto st = (*svc)(req, res);
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

}
