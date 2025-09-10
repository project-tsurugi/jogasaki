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

TEST_F(service_api_test, cancel_insert) {
    enable_request_cancel(request_cancel_kind::write);
    execute_statement("create table t (c0 int primary key)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_cancel_statement("insert into t values (1)", tx_handle);

    // Canceling sql makes tx undefined state, so the status is unknown by design.
    // But typically tx becomes inactive due to abort, so we use it here and similar testing
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

TEST_F(service_api_test, cancel_scan) {
    enable_request_cancel(request_cancel_kind::scan);
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (0)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_cancel_statement("select * from t order by c0", tx_handle);
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

TEST_F(service_api_test, cancel_find) {
    enable_request_cancel(request_cancel_kind::find);
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (0)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_cancel_statement("select * from t where c0 = 0", tx_handle);
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

TEST_F(service_api_test, cancel_group) {
    enable_request_cancel(request_cancel_kind::group);
    execute_statement("create table t0 (c0 int)");
    execute_statement("insert into t0 values (0)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("insert into t1 values (0)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_cancel_statement("select * from t0 join t1 on t0.c0 = t1.c0", tx_handle);
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

TEST_F(service_api_test, cancel_aggregate) {
    enable_request_cancel(request_cancel_kind::group);
    execute_statement("create table t0 (c0 int)");
    execute_statement("insert into t0 values (0)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_cancel_statement("select max(c0) from t0", tx_handle);
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

TEST_F(service_api_test, cancel_take_cogroup) {
    enable_request_cancel(request_cancel_kind::take_cogroup);
    execute_statement("create table t0 (c0 int)");
    execute_statement("insert into t0 values (0)");
    execute_statement("create table t1 (c0 int)");
    execute_statement("insert into t1 values (0)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_cancel_statement("select * from t0 join t1 on t0.c0 = t1.c0", tx_handle);
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

TEST_F(service_api_test, cancel_take_group) {
    enable_request_cancel(request_cancel_kind::take_group);
    execute_statement("create table t0 (c0 int)");
    execute_statement("insert into t0 values (0)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_cancel_statement("select max(c0) from t0", tx_handle);
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

TEST_F(service_api_test, cancel_tx_begin) {
    enable_request_cancel(request_cancel_kind::transaction_begin_wait);
    api::transaction_handle tx_handle{};
    test_cancel_transaction_begin(tx_handle, "label");
    // we don't have valid tx handle, so there is nothing to verify
}

TEST_F(service_api_test, cancel_take_flat) {
    enable_request_cancel(request_cancel_kind::take_flat);
    execute_statement("create table t0 (c0 int)");
    execute_statement("insert into t0 values (0)");
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_cancel_statement("select c0 from t0 limit 1", tx_handle);
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

TEST_F(service_api_test, DISABLED_cancel_precommit) {
    enable_request_cancel(request_cancel_kind::transaction_precommit);
    api::transaction_handle tx_handle{};
    test_cancel_transaction_begin(tx_handle, "label");
    test_cancel_transaction_commit(tx_handle, false);  // disable auto dispose
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

TEST_F(service_api_test, cancel_durable_wait) {
    if (jogasaki::kvs::implementation_id() == "memory") {
        GTEST_SKIP() << "jogasaki-memory doesn't call durability callback";
    }
    enable_request_cancel(request_cancel_kind::transaction_durable_wait);
    api::transaction_handle tx_handle{};
    test_begin(tx_handle);
    test_cancel_transaction_commit(tx_handle, false);  // disable auto dispose
    test_commit(tx_handle, false, error_code::inactive_transaction_exception); // verify tx is not usable
    test_get_error_info(tx_handle, false, error_code::none);  // tx in unknown state, so no error info.
}

}  // namespace jogasaki::api
