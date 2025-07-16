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

class comments_test :
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

TEST_F(comments_test, table) {
    auto sql = R"(
        /**
        * Example table.
        */
        CREATE TABLE example (

        /** The key column. */
        k INT PRIMARY KEY,

        /**
         * The value column.
         * default: ''
         */
        v VARCHAR(*) DEFAULT ''

        )
    )";
    execute_statement(sql);

    auto tables = global::database_impl()->tables();
    auto t = tables->find_table("example");
    ASSERT_TRUE(t);
    EXPECT_EQ("Example table.", t->description());
    auto& c0 = t->columns()[0];
    EXPECT_EQ("k", c0.simple_name());
    EXPECT_EQ("The key column.", c0.description());
    auto& c1 = t->columns()[1];
    EXPECT_EQ("v", c1.simple_name());
    EXPECT_EQ("The value column.\ndefault: ''", c1.description());
}

TEST_F(comments_test, index) {
    auto table_ddl = R"(
        /**
        * Example table.
        */
        CREATE TABLE t (

        /** The key column. */
        k INT PRIMARY KEY,

        /**
         * The value column.
         * default: ''
         */
        v VARCHAR(*) DEFAULT ''

        )
    )";
    execute_statement(table_ddl);
    auto index_ddl = R"(
        /**
        * Example index.
        */
        CREATE INDEX i ON t (v)
    )";
    execute_statement(index_ddl);

    auto tables = global::database_impl()->tables();
    auto t = tables->find_table("t");
    ASSERT_TRUE(t);
    auto i = tables->find_index("i");
    ASSERT_TRUE(i);
    EXPECT_EQ("Example index.", i->description());
    ASSERT_EQ(1, i->keys().size());
    auto& v = i->keys()[0].column();
    EXPECT_EQ("v", v.simple_name());
    EXPECT_EQ("The value column.\ndefault: ''", v.description());
}

}
