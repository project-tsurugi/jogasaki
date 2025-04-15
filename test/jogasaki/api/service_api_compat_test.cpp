/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <chrono>
#include <memory>
#include <string>
#include <string_view>
#include <type_traits>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <tateyama/api/configuration.h>
#include <tateyama/api/server/database_info.h>
#include <tateyama/api/server/mock/request_response.h>
#include <tateyama/proto/diagnostics.pb.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/service.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/compare_info.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/model/task.h>
#include <jogasaki/proto/sql/common.pb.h>
#include <jogasaki/proto/sql/request.pb.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/test_utils/temporary_folder.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/command_utils.h>

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

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;
using ft = meta::field_type_kind;

using jogasaki::api::impl::get_impl;

class service_api_compat_test :
    public ::testing::Test,
    public testing::api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
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

    std::shared_ptr<jogasaki::api::impl::service> service_{};  //NOLINT
    test::temporary_folder temporary_{};

};

TEST_F(service_api_compat_test, valid_version) {
    // verify by sending current major/minor version
    sql::request::Request r{};
    // using list tables as it's the simplest and most stable
    auto* lt = r.mutable_listtables();
    (void) lt;
    r.mutable_session_handle()->set_handle(1);

    r.set_service_message_version_major(service_message_version_major);
    r.set_service_message_version_minor(service_message_version_minor);

    auto s = utils::serialize(r);
    r.clear_listtables();

    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);
    auto result = decode_list_tables(res->body_);
    (void) result;
}

TEST_F(service_api_compat_test, invalid_version) {
    // verify by sending newer major version for current service
    sql::request::Request r{};
    auto* lt = r.mutable_listtables();
    (void) lt;
    r.mutable_session_handle()->set_handle(1);

    r.set_service_message_version_major(service_message_version_major+1);
    r.set_service_message_version_minor(0);

    auto s = utils::serialize(r);
    r.clear_listtables();

    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);
    auto record = res->error_;
    EXPECT_EQ(record.code(), tateyama::proto::diagnostics::Code::INVALID_REQUEST);
    EXPECT_EQ(record.message(),
              "inconsistent service message version: see https://github.com/project-tsurugi/tsurugidb/blob/master/docs/service-message-compatibilities.md (client: \"sql-2.0\", server: \"sql-1.5\")");
}

TEST_F(service_api_compat_test, none_version_provided) {
    // verify by sending request with no version info
    sql::request::Request r{};
    auto* lt = r.mutable_listtables();
    (void) lt;
    r.mutable_session_handle()->set_handle(1);

    r.clear_service_message_version_major();
    r.clear_service_message_version_minor();

    auto s = utils::serialize(r);
    r.clear_listtables();

    auto req = std::make_shared<tateyama::api::server::mock::test_request>(s);
    auto res = std::make_shared<tateyama::api::server::mock::test_response>();
    auto st = (*service_)(req, res);
    EXPECT_TRUE(res->wait_completion());
    ASSERT_TRUE(st);
    auto record = res->error_;
    EXPECT_EQ(record.code(), tateyama::proto::diagnostics::Code::INVALID_REQUEST);
    EXPECT_EQ(record.message(),
    "inconsistent service message version: see https://github.com/project-tsurugi/tsurugidb/blob/master/docs/service-message-compatibilities.md (client: \"sql-0.0\", server: \"sql-1.5\")"
    );
}
}
