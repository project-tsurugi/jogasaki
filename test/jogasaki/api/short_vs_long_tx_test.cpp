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
#include <vector>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class short_vs_long_tx_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->prepare_test_tables(true);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

// running two test result in invalid state

TEST_F(short_vs_long_tx_test, short) {
    auto tx = utils::create_transaction(*db_);
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 where C0=1", *tx, result);
    ASSERT_EQ(1, result.size());
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

TEST_F(short_vs_long_tx_test, long_simple) {
    auto tx = utils::create_transaction(*db_, false, true, {"T0"});
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 1.0)", *tx);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 where C0=1", *tx, result);
    ASSERT_EQ(1, result.size());
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (2, 2.0)", *tx);
    ASSERT_EQ(status::ok, tx->commit());
}

}
