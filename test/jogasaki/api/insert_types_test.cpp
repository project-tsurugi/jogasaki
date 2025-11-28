/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <boost/move/utility_core.hpp>
#include <gtest/gtest.h>

#include <takatori/util/downcast.h>

#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/port.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/add_test_tables.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class insert_types_test :
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
        utils::add_test_tables();
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(insert_types_test, insert) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    test_stmt_err("INSERT INTO T0 (C0, C1) VALUES (1, 20.0)", error_code::unique_constraint_violation_exception);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,10.0)), result[0]);
}

TEST_F(insert_types_test, insert_if_not_exists) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement("INSERT IF NOT EXISTS INTO T0 (C0, C1) VALUES (1, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,10.0)), result[0]);
}

TEST_F(insert_types_test, insert_or_ignore) {
    // alias of insert if not exists
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement("INSERT OR IGNORE INTO T0 (C0, C1) VALUES (1, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,10.0)), result[0]);
}

TEST_F(insert_types_test, insert_or_replace) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement("INSERT OR REPLACE INTO T0 (C0, C1) VALUES (1, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,20.0)), result[0]);
}

TEST_F(insert_types_test, update_or_insert) {
    execute_statement("INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");
    execute_statement("UPDATE OR INSERT INTO T0 (C0, C1) VALUES (1, 20.0)");
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 ORDER BY C0", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::int8, kind::float8>(1,20.0)), result[0]);
}
}
