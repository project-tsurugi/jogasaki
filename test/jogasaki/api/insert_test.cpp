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

#include <regex>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/executor/common/graph.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/data/any.h>

#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/kvs/id.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using decimal_v = takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class insert_test :
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

// regression test scenario - once updating sequence stuck on 4th insert
TEST_F(insert_test, pkless_insert) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("create table TT (C0 int, C1 int)");
    wait_epochs(1);
    execute_statement("INSERT INTO TT (C0, C1) VALUES (2,2)");
    wait_epochs(1);
    execute_statement("INSERT INTO TT (C0, C1) VALUES (2,2)");
    wait_epochs(1);
    execute_statement("INSERT INTO TT (C0, C1) VALUES (2,2)");
    wait_epochs(1);
    execute_statement("INSERT INTO TT (C0, C1) VALUES (2,2)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0 FROM TT", result);
        ASSERT_EQ(4, result.size());
    }
}

TEST_F(insert_test, insert_without_explicit_column) {
    execute_statement("create table T (C0 bigint, C1 double)");
    std::unique_ptr<api::executable_statement> stmt0{};
    ASSERT_EQ(status::ok, db_->create_executable("INSERT INTO T VALUES (1, 20.0)", stmt0));
    auto tx = utils::create_transaction(*db_);
    ASSERT_EQ(status::ok, tx->execute(*stmt0));
    ASSERT_EQ(status::ok, tx->commit());
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,20.0)), result[0]);
}

TEST_F(insert_test, pkless_insert_without_explicit_column) {
    utils::set_global_tx_option(utils::create_tx_option{false, true});
    execute_statement("create table TT (C0 int, C1 int)");
    execute_statement("INSERT INTO TT VALUES (2,20)");
    execute_statement("INSERT INTO TT VALUES (2,20)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT C0, C1 FROM TT", result);
        ASSERT_EQ(2, result.size());
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,20)), result[0]);
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2,20)), result[1]);
    }
}

}
