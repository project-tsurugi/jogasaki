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

class in_transaction_ops_test :
    public ::testing::Test,
    public api_test_base {

public:
    // change this flag to debug with explain
    bool to_explain() override {
        return false;
    }

    void SetUp() override {
        auto cfg = std::make_shared<configuration>();
        cfg->scan_default_parallel(2);
        db_setup(cfg);
    }

    void TearDown() override {
        db_teardown();
    }

    bool has_join_find(std::string_view query);
    bool has_join_scan(std::string_view query);

};

bool contains(std::string_view whole, std::string_view part) {
    return whole.find(part) != std::string_view::npos;
}

bool in_transaction_ops_test::has_join_find(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "join_find");
}
bool in_transaction_ops_test::has_join_scan(std::string_view query) {
    std::string plan{};
    explain_statement(query, plan);
    return contains(plan, "join_scan");
}

using namespace std::string_view_literals;

TEST_F(in_transaction_ops_test, scan) {
    // manually verify rtx scan runs as in-transaction task
    // strand handles are assigned and used to accommodate multiple threads to run scan
    execute_statement("CREATE TABLE t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (100), (200), (300)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t", *tx, result);
    std::sort(result.begin(), result.end());
    ASSERT_EQ(3, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4>(200)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4>(300)), result[2]);
}

TEST_F(in_transaction_ops_test, find) {
    // verify rtx find runs as in-transaction task
    // strand handles are assigned and used to accommodate multiple threads to run find
    execute_statement("CREATE TABLE t (c0 int primary key)");
    execute_statement("INSERT INTO t VALUES (100), (200), (300)");
    auto tx = utils::create_transaction(*db_, true, false);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM t WHERE c0 = 200", *tx, result);
    std::sort(result.begin(), result.end());
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int4>(200)), result[0]);
}

TEST_F(in_transaction_ops_test, join_find) {
    // manually verify rtx join_find runs as in-transaction task
    // strand handles are assigned and used to accommodate multiple threads to run join_find
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 int primary key, c1 int)");
    execute_statement("INSERT INTO t1 VALUES (1, 10)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_find(query));
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_, true, false);
    execute_query(query, *tx, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 10)), result[0]);
}

TEST_F(in_transaction_ops_test, join_scan) {
    // manually verify rtx join_scan runs as in-transaction task
    // strand handles are assigned and used to accommodate multiple threads to run join_scan
    execute_statement("CREATE TABLE t0 (c0 int)");
    execute_statement("INSERT INTO t0 VALUES (1),(2)");
    execute_statement("CREATE TABLE t1 (c0 int, c1 int, primary key(c0, c1))");
    execute_statement("INSERT INTO t1 VALUES (1,10),(3,30)");

    auto query = "SELECT t0.c0, t1.c0, t1.c1 FROM t0 join t1 on t0.c0=t1.c0";
    EXPECT_TRUE(has_join_scan(query));
    std::vector<mock::basic_record> result{};
    auto tx = utils::create_transaction(*db_, true, false);
    execute_query(query, *tx, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((mock::create_nullable_record<kind::int4, kind::int4, kind::int4>(1, 1, 10)), result[0]);
}

} // namespace jogasaki::testing
