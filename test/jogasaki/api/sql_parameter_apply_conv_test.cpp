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

#include <takatori/datetime/date.h>
#include <takatori/datetime/time_of_day.h>
#include <takatori/datetime/time_point.h>
#include <takatori/decimal/triple.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/decimal.h>
#include <takatori/type/octet.h>
#include <takatori/type/primitive.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/type_kind.h>
#include <takatori/type/varying.h>
#include <takatori/util/downcast.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>
#include <yugawara/util/maybe_shared_lock.h>

#include <jogasaki/accessor/text.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/parameter_set.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/configuration.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/port.h>
#include <jogasaki/executor/function/builtin_scalar_functions_id.h>
#include <jogasaki/executor/function/field_locator.h>
#include <jogasaki/executor/function/scalar_function_info.h>
#include <jogasaki/executor/function/scalar_function_kind.h>
#include <jogasaki/executor/function/scalar_function_repository.h>
#include <jogasaki/executor/function/value_generator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/decimal_field_option.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/meta/type_helper.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/hybrid_execution_mode.h>
#include <jogasaki/utils/add_test_tables.h>
#include <jogasaki/utils/create_tx.h>
#include <jogasaki/utils/tables.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::meta;
using namespace jogasaki::model;
using namespace jogasaki::executor;
using namespace jogasaki::scheduler;
using namespace jogasaki::mock;

using triple = takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;
using executor::expr::evaluator_context;
using jogasaki::executor::expr::error;
using jogasaki::executor::expr::error_kind;
using takatori::util::sequence_view;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;


class sql_parameter_apply_conv_test :
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
        mock::basic_record::compare_decimals_as_triple_ = false;  // reset global flag
    }
};

using namespace std::string_view_literals;
using namespace jogasaki::executor::function;

namespace t = takatori::type;
using namespace ::yugawara;

TEST_F(sql_parameter_apply_conv_test, verify_parameter_application_conversion) {
    // no count(char) is registered, but count(varchar) is applied instead for char columns
    // by paramater application conversion
    execute_statement("create table t (c0 char(3))");
    execute_statement("insert into t values ('aaa'), ('bbb'), ('ccc')");
    {
        std::vector<mock::basic_record> result{};
        execute_query("SELECT COUNT(c0) FROM t", result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<kind::int8>(3)), result[0]);
    }
}

TEST_F(sql_parameter_apply_conv_test, substr_int) {
    // regression testcase for issue 1367 - passing int parameter to substr was broken
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 varchar(5))");
    execute_statement("insert into t values ('ABC')");
    execute_query("SELECT substr(c0, 1::int, 1) FROM t", result);
    ASSERT_EQ(1, result.size());
    EXPECT_EQ((create_nullable_record<kind::character>(accessor::text{"A"})), result[0]);
}

TEST_F(sql_parameter_apply_conv_test, conversion_preserves_precision) {
    // regression testcase - 1.00/1.10 converted to 1/1.1 accidentally
    mock::basic_record::compare_decimals_as_triple_ = true;
    std::vector<mock::basic_record> result{};
    execute_statement("create table t (c0 decimal(5,2))");
    execute_statement("insert into t values (1.00),(1.10)");
    execute_query("SELECT abs(c0) FROM t", result);
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());
    EXPECT_EQ((typed_nullable_record<kind::decimal>(std::tuple{decimal_type()}, triple{1, 0, 100, -2})), result[0]);
    EXPECT_EQ((typed_nullable_record<kind::decimal>(std::tuple{decimal_type()}, triple{1, 0, 110, -2})), result[1]);
}

}  // namespace jogasaki::testing
