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

#include <memory>
#include <string>
#include <string_view>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using date_v = takatori::datetime::date;
using time_of_day_v = takatori::datetime::time_of_day;
using time_point_v = takatori::datetime::time_point;
using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;
using kind = meta::field_type_kind;
using api::impl::get_impl;

class create_index_test:
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(create_index_test, blocked_by_existing_records) {
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES(1,1)");
    test_stmt_err("CREATE INDEX I ON T (C1)", error_code::unsupported_runtime_feature_exception);
}

TEST_F(create_index_test, ddl_error_aborts_tx) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    execute_statement("INSERT INTO T VALUES(1,1)");
    auto tx = utils::create_transaction(*db_);
    test_stmt_err("CREATE INDEX I ON T (C1)", *tx, error_code::unsupported_runtime_feature_exception);
    ASSERT_EQ(status::err_inactive_transaction, tx->commit());
}

TEST_F(create_index_test, multiple_ddls_using_same_tx) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY, C1 INT)");
    auto tx = utils::create_transaction(*db_);
    execute_statement("CREATE INDEX I0 ON T (C1)", *tx);
    execute_statement("CREATE INDEX I1 ON T (C1)", *tx);
    execute_statement("CREATE INDEX I2 ON T (C1)", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

}  // namespace jogasaki::testing
