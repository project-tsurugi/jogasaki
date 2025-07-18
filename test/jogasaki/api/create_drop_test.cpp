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

#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
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

/**
 * @brief regression testcase - DDL affected by introducing commit callback
 */
class create_drop_test:
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

TEST_F(create_drop_test, create0) {
    utils::set_global_tx_option({false, true}); // to customize
    execute_statement("CREATE TABLE T (C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO T (C0) VALUES(1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T", result);
    }
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name("T");
    ASSERT_TRUE(e.has_value());
    auto s = smgr.find_entry(e.value());
    ASSERT_TRUE(s);
}

TEST_F(create_drop_test, drop0) {
    utils::set_global_tx_option({true, false}); // to customize
    execute_statement("CREATE TABLE TT (C0 INT NOT NULL PRIMARY KEY)");
    execute_statement("INSERT INTO TT (C0) VALUES(1)");
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name("TT");
    ASSERT_TRUE(e.has_value());
    auto s = smgr.find_entry(e.value());
    ASSERT_TRUE(s);
    execute_statement("DROP TABLE TT");
    ASSERT_TRUE(! smgr.find_by_name("TT").has_value());
    ASSERT_TRUE(! smgr.find_entry(e.value()));
    execute_statement("CREATE TABLE TT2 (C0 INT NOT NULL PRIMARY KEY)");
    auto e2 = smgr.find_by_name("TT2");
    ASSERT_TRUE(e2.has_value());
    ASSERT_TRUE(! smgr.find_entry(e.value())); // TT2 id must be different from TT id, should not be recycled
    execute_statement("INSERT INTO TT2 (C0) VALUES(1)");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM TT2", result);
        ASSERT_EQ(1, result.size());
    }
}

}
