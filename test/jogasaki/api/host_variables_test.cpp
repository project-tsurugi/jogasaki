/*
 * Copyright 2018-2020 tsurugi project.
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
#include <jogasaki/executor/process/impl/expression/any.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/utils/mock/storage_data.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/transaction.h>
#include <jogasaki/api/result_set.h>
#include <jogasaki/api/impl/record.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/executor/tables.h>
#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;

using takatori::util::unsafe_downcast;

class host_variables_test :
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

        auto* impl = db_impl();
        add_benchmark_tables(*impl->tables());
        register_kvs_storage(*impl->kvs_db(), *impl->tables());
    }

    void TearDown() override {
        db_teardown();
    }
};

using namespace std::string_view_literals;

TEST_F(host_variables_test, insert_host_variable) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
    };
    auto ps = api::create_parameter_set();
    ps->set_int8("p0", 1);
    ps->set_float8("p1", 10.0);
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (:p0, :p1)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(10.0, result[0].get_value<double>(1));
}

TEST_F(host_variables_test, update_host_variable) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
        {"i0", api::field_type_kind::int8},
        {"i1", api::field_type_kind::int8},
    };
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (1, 10.0)");

    {
        auto ps = api::create_parameter_set();
        ps->set_int8("p0", 1);
        ps->set_float8("p1", 20.0);
        execute_statement( "UPDATE T0 SET C1 = :p1 WHERE C0 = :p0", variables, *ps);

        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
        EXPECT_DOUBLE_EQ(20.0, result[0].get_value<double>(1));
    }
    {
        auto ps = api::create_parameter_set();
        ps->set_int8("i0", 1);
        ps->set_int8("i1", 2);
        execute_statement( "UPDATE T0 SET C0 = :i1 WHERE C0 = :i0", variables, *ps);

        std::vector<mock::basic_record> result{};
        execute_query("SELECT * FROM T0", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ(2, result[0].get_value<std::int64_t>(0));
        EXPECT_DOUBLE_EQ(20.0, result[0].get_value<double>(1));
    }
}

TEST_F(host_variables_test, query_host_variable) {
    std::unordered_map<std::string, api::field_type_kind> variables{
        {"p0", api::field_type_kind::int8},
        {"p1", api::field_type_kind::float8},
    };
    auto ps = api::create_parameter_set();
    ps->set_int8("p0", 1);
    ps->set_float8("p1", 10.0);
    execute_statement( "INSERT INTO T0 (C0, C1) VALUES (:p0, :p1)", variables, *ps);
    std::vector<mock::basic_record> result{};
    execute_query("SELECT * FROM T0 WHERE C0 = :p0 AND C1 = :p1", variables, *ps, result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ(1, result[0].get_value<std::int64_t>(0));
    EXPECT_DOUBLE_EQ(10.0, result[0].get_value<double>(1));
}

}
