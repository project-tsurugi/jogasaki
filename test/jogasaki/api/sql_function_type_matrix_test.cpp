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

using decimal_v = takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;

class sql_function_type_matrix_test :
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

    template<kind ResultKind>
    void test_function_with_type(
        std::string_view fn_name,
        std::string_view type_name,
        std::string_view values,
        std::optional<runtime_t<ResultKind>> exp
    );
};

using namespace std::string_view_literals;

template<kind ResultKind>
void sql_function_type_matrix_test::test_function_with_type(
    std::string_view fn_name,
    std::string_view type_name,
    std::string_view values,
    std::optional<runtime_t<ResultKind>> exp
) {
    execute_statement("create table t (c0 "+std::string{type_name}+")");
    execute_statement("insert into t values "+std::string{values}+"");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT "+std::string{fn_name}+"c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        if(exp.has_value()) {
            EXPECT_EQ((create_nullable_record<ResultKind>(*exp)), result[0]);
        } else {
            EXPECT_EQ((create_nullable_record<ResultKind>({}, {false})), result[0]);
        }
    }
}

//////////////////
// count
//////////////////

TEST_F(sql_function_type_matrix_test, count_int) {
    test_function_with_type<kind::int8>("count(", "INT", "(1),(2),(3)", 3);
}

TEST_F(sql_function_type_matrix_test, count_varchar) {
    test_function_with_type<kind::int8>("count(", "VARCHAR", "('AAA'),('BBB'),('CCC')", 3);
}

//////////////////
// count distinct
//////////////////
TEST_F(sql_function_type_matrix_test, count_distinct_int) {
    test_function_with_type<kind::int8>("count(distinct ", "INT", "(1),(1),(2)", 2);
}

//////////////////
// max
//////////////////

TEST_F(sql_function_type_matrix_test, max_int) {
    test_function_with_type<kind::int4>("max(", "INT", "(1),(2),(3)", 3);
}

}  // namespace jogasaki::testing
