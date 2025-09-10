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
#include "service_api_common.h"

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

TEST_F(service_api_test, extract_sql) {
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

TEST_F(service_api_test, extract_prepared_sql) {
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

TEST_F(service_api_test, extract_sql_error) {
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

TEST_F(service_api_test, extract_sql_failing_to_fetch_tx_id) {
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

TEST_F(service_api_test, fail_to_extract_sql_on_different_session) {
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

TEST_F(service_api_test, fail_to_extract_tx_on_different_session) {
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
