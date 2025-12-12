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

#include "../api/api_test_base.h"
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

TEST_F(service_api_test, blob_types) {
    // global::config_pool()->mock_datastore(true);
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into t values (0, :p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::BLOB},
        std::pair{"p1"s, sql::common::AtomType::CLOB}
    );

    auto path0 = path() + "/blob0.dat";
    auto path1 = path() + "/clob1.dat";
    create_file(path0, "ABC");
    create_file(path1, "DEF");
    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kBlob, std::any{std::in_place_type<lob::blob_locator>, lob::blob_locator{path0, false}}},
            {"p1"s, ValueCase::kClob, std::any{std::in_place_type<lob::clob_locator>, lob::clob_locator{path1, false}}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        req->add_blob(path0, std::make_shared<tateyama::api::server::mock::test_blob_info>(path0, path0, false));
        req->add_blob(path1, std::make_shared<tateyama::api::server::mock::test_blob_info>(path1, path1, false));
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto [success, error, stats] = decode_execute_result(res->body_);
        ASSERT_TRUE(success);
    }
    test_commit(tx_handle);
    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select c1, c2 from t"
    );
    test_begin(tx_handle);
    {
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"c1", common_column::atom_type::blob},   // nullable is not sent now
                {"c2", common_column::atom_type::clob},   // nullable is not sent now
            };
            ASSERT_EQ(exp, cols);

            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());

                auto v0 = v[0].get_value<lob::blob_reference>(0);
                auto v1 = v[0].get_value<lob::clob_reference>(1);

                EXPECT_EQ((mock::typed_nullable_record<ft::blob, ft::clob>(
                    std::tuple{meta::blob_type(), meta::clob_type()},
                    {
                        lob::blob_reference{v0.object_id(), lob::lob_data_provider::datastore},
                        lob::clob_reference{v1.object_id(), lob::lob_data_provider::datastore},
                    },
                    {false, false}
                )), v[0]);

                auto* ds = datastore::get_datastore();
                auto f0 = ds->get_blob_file(v0.object_id());
                auto f1 = ds->get_blob_file(v1.object_id());
                ASSERT_TRUE(f0);
                ASSERT_TRUE(f1);
                EXPECT_EQ("ABC", read_file(f0.path().string()));
                EXPECT_EQ("DEF", read_file(f1.path().string()));

                {
                    // test_get_lob(v0.object_id(), v0.reference_tag(), f0.path().string());
                    test_get_lob(v0.object_id(), 1, f0.path().string()); //FIXME currently any reference tag is accepted
                    test_get_lob(v1.object_id(), 0, f1.path().string());
                }
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

TEST_F(service_api_test, blob_types_error_handling) {
    global::config_pool()->mock_datastore(true);
    datastore::get_datastore(true); // reset cache for mock
    execute_statement("create table t (c0 int primary key, c1 blob, c2 clob)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    std::uint64_t stmt_handle{};
    test_prepare(
        stmt_handle,
        "insert into t values (0, :p0, :p1)",
        std::pair{"p0"s, sql::common::AtomType::BLOB},
        std::pair{"p1"s, sql::common::AtomType::CLOB}
    );

    auto path0 = path() + "/" + std::string{datastore::datastore_mock::file_name_to_raise_io_exception} + ".dat";
    auto path1 = path() + "/clob1.dat";
    if (global::config_pool()->mock_datastore()) {
        // mock raises io exception by the file name, while prod raises by detecting missing file
        create_file(path0, "ABC");
    }
    create_file(path1, "DEF");
    {
        std::vector<parameter> parameters{
            {"p0"s, ValueCase::kBlob, std::any{std::in_place_type<lob::blob_locator>, lob::blob_locator{path0, false}}},
            {"p1"s, ValueCase::kClob, std::any{std::in_place_type<lob::clob_locator>, lob::clob_locator{path1, false}}},
        };
        auto s = encode_execute_prepared_statement(tx_handle, stmt_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        req->add_blob(path0, std::make_shared<tateyama::api::server::mock::test_blob_info>(path0, path0, false));
        req->add_blob(path1, std::make_shared<tateyama::api::server::mock::test_blob_info>(path1, path1, false));
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto& rec = res->error_;
        EXPECT_EQ(::tateyama::proto::diagnostics::Code::IO_ERROR, rec.code());
        std::cerr << "error:" << rec.message() << std::endl;
    }
    test_dispose_prepare(stmt_handle);
}

TEST_F(service_api_test, blob_types_error_sending_back_unprivileded) {
    // verify error when blob is returned on non-priviledged mode
    global::config_pool()->mock_datastore(true);
    global::config_pool()->enable_blob_cast(true);
    datastore::get_datastore(true); // reset cache for mock
    execute_statement("create table t (c0 int primary key, c1 blob)");
    execute_statement("insert into t values (0, x'000102')");

    std::uint64_t query_handle{};
    test_prepare(
        query_handle,
        "select c1 from t"
    );
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    lob::lob_id_type id{};
    {
        // run query to get blob id
        std::vector<parameter> parameters{};
        auto s = encode_execute_prepared_query(tx_handle, query_handle, parameters);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        {
            auto [name, cols] = decode_execute_query(res->body_head_);
            std::vector<common_column> exp{
                {"c1", common_column::atom_type::blob},  // nullable is not sent now
            };
            ASSERT_EQ(exp, cols);

            {
                ASSERT_TRUE(res->channel_);
                auto& ch = *res->channel_;
                auto m = create_record_meta(cols);
                auto v = deserialize_msg(ch.view(), m);
                ASSERT_EQ(1, v.size());

                id = v[0].get_value<lob::blob_reference>(0).object_id();
            }
        }
    }
    {
        // get blob data using the id
        // reference_tag is no longer stored in lob_reference, pass 0
        auto s = encode_get_large_object_data(id, 0);

        auto req = std::make_shared<tateyama::api::server::mock::test_request>(s, session_id_);
        auto res = std::make_shared<tateyama::api::server::mock::test_response>();
        res->set_privileged(false);

        auto st = (*service_)(req, res);
        EXPECT_TRUE(res->wait_completion());
        EXPECT_TRUE(res->completed());
        ASSERT_TRUE(st);

        auto& rec = res->error_;
        EXPECT_EQ(::tateyama::proto::diagnostics::Code::OPERATION_DENIED, rec.code());
        std::cerr << "error:" << rec.message() << std::endl;
    }
    test_commit(tx_handle);
    test_dispose_prepare(query_handle);
}

}  // namespace jogasaki::api
