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

#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/downcast.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
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

using takatori::decimal::triple;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class parallel_scan_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->rtx_parallel_scan(true);
        cfg->scan_default_parallel(5);
        cfg->key_distribution(key_distribution_kind::uniform);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(parallel_scan_test, simple) {
    // manually verify the log message and check pivots for stored data
    execute_statement("CREATE TABLE t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (100), (200), (300)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    ASSERT_EQ(3, result.size());
}

TEST_F(parallel_scan_test, empty_table) {
    // verify no crash when table is empty for parallel scan
    execute_statement("CREATE TABLE t (c0 int primary key)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    ASSERT_EQ(0, result.size());
}

TEST_F(parallel_scan_test, negative_values) {
    // test with negative values
    execute_statement("CREATE TABLE t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (-100),(-200)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    ASSERT_EQ(2, result.size());
}

TEST_F(parallel_scan_test, various_types) {
    // test with various types and check no crash
    execute_statement("CREATE TABLE t (c0 int, c1 bigint, c2 char(20), c3 varchar(20), c4 real, c5 double, c6 decimal(5,3), primary key(c0, c1, c2, c3, c4, c5, c6))");
    execute_statement("INSERT INTO t VALUES (-1, -1, '', '', -1.0, -1.0, -1)");
    execute_statement("INSERT INTO t VALUES (10, 10, '11111111111111111111', '11111111111111111111', 10.0, 10.0, 10)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    ASSERT_EQ(2, result.size());
}
}
