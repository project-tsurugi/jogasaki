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
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/constants.h>
#include <jogasaki/datastore/datastore_mock.h>
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

TEST_F(service_api_test, execute_dump_load) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    test_dump(files);
    test_statement("delete from T0");
    std::stringstream ss{};
    for(auto&& s : files) {
        ss << s;
        ss << " ";
    }
    LOG(INFO) << "dump files: " << ss.str();
    test_load(true, error_code::none, files[0]);
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
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    test_dump(files);
    test_statement("delete from T0");
    std::stringstream ss{};
    for(auto&& s : files) {
        ss << s;
        ss << " ";
    }
    LOG(INFO) << "dump files: " << ss.str();
    test_load(false, error_code::none, files[0]);
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
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    test_dump(files, "/dummy_path", error_code::sql_execution_exception);
}

TEST_F(service_api_test, dump_error_with_query_result) {
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    // test if error in the middle of query processing is handled correctly
    test_statement("insert into T0(C0, C1) values (1, 10.0)");
    test_statement("insert into T0(C0, C1) values (2, 0.0)");
    test_statement("insert into T0(C0, C1) values (3, 30.0)");
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select C0, 1.0/C1 from T0"
    );
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    do {
        auto s = encode_execute_dump(tx_handle, query_handle, {}, std::string{service_api_test::temporary_.path()});

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        EXPECT_TRUE(res->all_released());
        ASSERT_TRUE(st);
        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"file_name", common_column::atom_type::character},  // nullable is not sent now
            };
            exp[0].varying_ = true;
            ASSERT_EQ(exp, cols);
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

            ASSERT_EQ(error_code::value_evaluation_exception, error.code_);
        }
    } while(0);
    test_commit(tx_handle);
    test_dispose_prepare(query_handle);
}

TEST_F(service_api_test, load_no_file) {
    // no file is specified - success
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    test_load(true, error_code::none);
}

TEST_F(service_api_test, DISABLED_load_no_file_non_tx) {
    // no file is specified - success
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    test_load(false, error_code::none);
}

TEST_F(service_api_test, load_empty_file_name) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    test_load(true, error_code::sql_execution_exception, "");
}

TEST_F(service_api_test, load_empty_file_name_non_tx) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    test_load(false, error_code::load_file_exception, "");
}

TEST_F(service_api_test, load_missing_files) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    test_load(true, error_code::sql_execution_exception, "dummy1.parquet", "dummy2.parquet");
}
TEST_F(service_api_test, load_missing_files_non_tx) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory has problem aborting tx from different threads";
    }
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    test_load(true, error_code::sql_execution_exception, "dummy1.parquet", "dummy2.parquet");
}

TEST_F(service_api_test, tx_load_invalid_handle) {
    // verify error returned from transactional load for invalid statement handle
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0 (C0, C1) values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::INT8},
        std::pair{"p1"s, sql::common::AtomType::FLOAT8}
    );
    test_dispose_prepare(stmt_handle);
    test_load(true, stmt_handle, error_code::statement_not_found_exception);
}

TEST_F(service_api_test, non_tx_load_invalid_handle) {
    // verify error returned from non-transactional load for invalid statement handle
    execute_statement("create table T0 (C0 bigint primary key, C1 double)");
    std::vector<std::string> files{};
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into T0 (C0, C1) values (:p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::INT8},
        std::pair{"p1"s, sql::common::AtomType::FLOAT8}
    );
    test_dispose_prepare(stmt_handle);
    test_load(false, stmt_handle, error_code::statement_not_found_exception);
}

}  // namespace jogasaki::api
