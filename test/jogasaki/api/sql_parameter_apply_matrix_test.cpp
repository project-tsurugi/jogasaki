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

using decimal_v = takatori::decimal::triple;
using date = takatori::datetime::date;
using time_of_day = takatori::datetime::time_of_day;
using time_point = takatori::datetime::time_point;
using executor::expr::evaluator_context;
using jogasaki::executor::expr::error;
using jogasaki::executor::expr::error_kind;
using takatori::util::sequence_view;
using takatori::util::unsafe_downcast;

using kind = meta::field_type_kind;


// test parameter application conversion with supported type matrix
class sql_parameter_apply_matrix_test :
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
    template <kind OutValueKind>
    void test_parameter_apply_conv(
        takatori::type::data&& type,
        std::string_view fn_input,
        bool expect_error
    );
};

using namespace std::string_view_literals;

using namespace jogasaki::executor::function;

template <kind OutValueKind>
void sql_parameter_apply_matrix_test::test_parameter_apply_conv(
    takatori::type::data&& type,
    std::string_view fn_input,
    bool expect_error
) {
    namespace t = takatori::type;
    using namespace ::yugawara;
    bool called = false;

    auto id = 5000000UL;  // any value to avoid conflict

    global::scalar_function_repository().add(
        id,
        std::make_shared<scalar_function_info>(
            scalar_function_kind::mock_function_for_testing,
            [&](evaluator_context&, sequence_view<data::any> arg) -> data::any {
                called = true;
                return arg[0];
            },
            1
        )
    );
    auto* in = type.clone();
    auto* out = type.clone();
    auto decl = global::scalar_function_provider()->add({
        id,
        "identity_fn",
        std::move(*out),
        {
            std::move(*in)
        },
    });
    delete in;
    delete out;
    execute_statement("create table t (c0 int primary key)");
    execute_statement("insert into t values (1)");
    auto sql = "SELECT identity_fn("+std::string{fn_input}+") FROM t";
    if (expect_error) {
        test_stmt_err(sql, error_code::symbol_analyze_exception);
        global::scalar_function_repository().clear();
        global::scalar_function_provider()->remove(*decl);
        return;
    }
    {
        std::vector<mock::basic_record> result{};
        execute_query(sql, result);
        ASSERT_EQ(1, result.size());
        EXPECT_EQ((create_nullable_record<OutValueKind>(1, {false})), result[0]);
    }
    EXPECT_TRUE(called);
    global::scalar_function_repository().clear();
    global::scalar_function_provider()->remove(*decl);
}

namespace t = takatori::type;
using namespace ::yugawara;

TEST_F(sql_parameter_apply_matrix_test, int4_to_int4) {
    test_parameter_apply_conv<kind::int4>(t::int4(), "1::int", false);
}

TEST_F(sql_parameter_apply_matrix_test, int4_to_int8) {
    test_parameter_apply_conv<kind::int8>(t::int8(), "1::int", false);
}

TEST_F(sql_parameter_apply_matrix_test, int4_to_decimal) {
    test_parameter_apply_conv<kind::decimal>(t::decimal(), "1::int", false);
}

TEST_F(sql_parameter_apply_matrix_test, int4_to_float4) {
    test_parameter_apply_conv<kind::float4>(t::float4(), "1::int", false);
}

TEST_F(sql_parameter_apply_matrix_test, int4_to_float8) {
    test_parameter_apply_conv<kind::float8>(t::float8(), "1::int", false);
}

// from int8

TEST_F(sql_parameter_apply_matrix_test, int8_to_int4_err) {
    test_parameter_apply_conv<kind::int4>(t::int4(), "1::bigint", true);
}

TEST_F(sql_parameter_apply_matrix_test, int8_to_int8) {
    test_parameter_apply_conv<kind::int8>(t::int8(), "1::bigint", false);
}

TEST_F(sql_parameter_apply_matrix_test, int8_to_decimal) {
    test_parameter_apply_conv<kind::decimal>(t::decimal(), "1::bigint", false);
}

TEST_F(sql_parameter_apply_matrix_test, int8_to_float4) {
    test_parameter_apply_conv<kind::float4>(t::float4(), "1::bigint", false);
}

TEST_F(sql_parameter_apply_matrix_test, int8_to_float8) {
    test_parameter_apply_conv<kind::float8>(t::float8(), "1::bigint", false);
}

// from decimal
TEST_F(sql_parameter_apply_matrix_test, decimal_to_int4_err) {
    test_parameter_apply_conv<kind::int4>(t::int4(), "1::decimal", true);
}

TEST_F(sql_parameter_apply_matrix_test, decimal_to_int8_err) {
    test_parameter_apply_conv<kind::int8>(t::int8(), "1::decimal", true);
}

TEST_F(sql_parameter_apply_matrix_test, decimal_to_decimal) {
    test_parameter_apply_conv<kind::decimal>(t::decimal(), "1::decimal", false);
}

TEST_F(sql_parameter_apply_matrix_test, decimal_to_float4) {
    test_parameter_apply_conv<kind::float4>(t::float4(), "1::decimal", false);
}

TEST_F(sql_parameter_apply_matrix_test, decimal_to_float8) {
    test_parameter_apply_conv<kind::float8>(t::float8(), "1::decimal", false);
}

// from float4

TEST_F(sql_parameter_apply_matrix_test, float4_to_int4_err) {
    test_parameter_apply_conv<kind::int4>(t::int4(), "1::real", true);
}

TEST_F(sql_parameter_apply_matrix_test, float4_to_int8_err) {
    test_parameter_apply_conv<kind::int8>(t::int8(), "1::real", true);
}

TEST_F(sql_parameter_apply_matrix_test, float4_to_decimal_err) {
    test_parameter_apply_conv<kind::decimal>(t::decimal(), "1::real", true);
}

TEST_F(sql_parameter_apply_matrix_test, float4_to_float4) {
    test_parameter_apply_conv<kind::float4>(t::float4(), "1::real", false);
}

TEST_F(sql_parameter_apply_matrix_test, float4_to_float8) {
    test_parameter_apply_conv<kind::float8>(t::float8(), "1::real", false);
}

// from float8

TEST_F(sql_parameter_apply_matrix_test, float8_to_int4_err) {
    test_parameter_apply_conv<kind::int4>(t::int4(), "1::double", true);
}

TEST_F(sql_parameter_apply_matrix_test, float8_to_int8_err) {
    test_parameter_apply_conv<kind::int8>(t::int8(), "1::double", true);
}

TEST_F(sql_parameter_apply_matrix_test, float8_to_decimal_err) {
    test_parameter_apply_conv<kind::decimal>(t::decimal(), "1::double", true);
}

TEST_F(sql_parameter_apply_matrix_test, float8_to_float4_err) {
    test_parameter_apply_conv<kind::float4>(t::float4(), "1::double", true);
}

TEST_F(sql_parameter_apply_matrix_test, float8_to_float8) {
    test_parameter_apply_conv<kind::float8>(t::float8(), "1::double", false);
}

}  // namespace jogasaki::testing
