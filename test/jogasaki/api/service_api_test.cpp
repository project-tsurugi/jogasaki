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

TEST_F(service_api_test, basic) {
    ::request::Request r{};
    ::request::Begin b;
    r.set_allocated_begin(&b);
    ::common::Session session;
    session.set_handle(0);
    r.set_allocated_session_handle(&session);

    std::string s{};
    if (!r.SerializeToString(&s)) { std::abort(); }
    std::cout << " debug : " << r.DebugString() << std::endl;
    r.release_begin();
    r.release_session_handle();
    std::cout << " data: " << utils::binary_printer{s.data(), s.size()} << std::endl;

    auto req = std::make_shared<tateyama::api::endpoint::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::endpoint::mock::test_response>();

    auto svc = tateyama::api::endpoint::create_service(*db_);

    auto st = (*svc)(req, res);
    ASSERT_EQ(tateyama::status::ok, st);

    ASSERT_EQ(response_code::success, res->code_);
}

}
