/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

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
#include <takatori/type/table.h>
#include <takatori/type/time_of_day.h>
#include <takatori/type/time_point.h>
#include <takatori/type/varying.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>

#include <jogasaki/accessor/binary.h>
#include <jogasaki/accessor/text.h>
#include <jogasaki/configuration.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/mock/basic_record.h>
#include <jogasaki/status.h>
#include <jogasaki/test_utils/mock_any_sequence_stream.h>
#include <jogasaki/utils/create_tx.h>

#include "api_test_base.h"

namespace jogasaki::testing {

using namespace std::literals::string_literals;
using namespace jogasaki;
using namespace jogasaki::mock;
using takatori::util::sequence_view;
using executor::expr::evaluator_context;
using namespace executor::function;
namespace t = takatori::type;

using kind = meta::field_type_kind;

namespace {

}  // namespace

/**
 * @brief test for APPLY operator with type matrix.
 * @details this test verifies that all data types can be used as arguments and return values
 *          for table-valued functions in APPLY operations.
 * Actually, APPLY operator has not very type-specific logic, but as the end-to-end test, we verify all types here.
 */
class sql_apply_type_matrix_test :
    public ::testing::Test,
    public api_test_base {

public:
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

TEST_F(sql_apply_type_matrix_test, int4_type) {
    execute_statement("CREATE TABLE T (C0 INT)");
    execute_statement("INSERT INTO T VALUES (100)");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12010;
    auto mock_table_func_int4_type = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        std::int32_t value = 0;
        if (! args.empty() && args[0]) {
            value = args[0].to<std::int32_t>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int32_t>, value}
        });
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int32_t>, value + 1}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_int4_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int4>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::int4>()},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_int4_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_int4_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(100, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(100, 101)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, int8_type) {
    execute_statement("CREATE TABLE T (C0 BIGINT)");
    execute_statement("INSERT INTO T VALUES (1000000000)");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12011;
    auto mock_table_func_int8_type = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        std::int64_t value = 0;
        if (! args.empty() && args[0]) {
            value = args[0].to<std::int64_t>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int64_t>, value}
        });
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int64_t>, value + 1}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_int8_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int8>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::int8>()},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_int8_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_int8_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1000000000, 1000000000)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int8, kind::int8>(1000000000, 1000000001)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, float4_type) {
    execute_statement("CREATE TABLE T (C0 REAL)");
    execute_statement("INSERT INTO T VALUES (1.5)");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12012;
    auto mock_table_func_float4_type = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        float value = 0.0F;
        if (! args.empty() && args[0]) {
            value = args[0].to<float>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<float>, value}
        });
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<float>, value + 1.0F}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_float4_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::float4>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::float4>()},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_float4_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_float4_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::float4, kind::float4>(1.5F, 1.5F)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::float4, kind::float4>(1.5F, 2.5F)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, float8_type) {
    execute_statement("CREATE TABLE T (C0 DOUBLE)");
    execute_statement("INSERT INTO T VALUES (2.5)");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12013;
    auto mock_table_func_float8_type = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        double value = 0.0;
        if (! args.empty() && args[0]) {
            value = args[0].to<double>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<double>, value}
        });
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<double>, value + 1.0}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_float8_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::float8>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::float8>()},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_float8_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_float8_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::float8, kind::float8>(2.5, 2.5)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::float8, kind::float8>(2.5, 3.5)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, decimal_type) {
    execute_statement("CREATE TABLE T (C0 DECIMAL(10, 2))");
    execute_statement("INSERT INTO T VALUES (123.45)");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12014;
    auto mock_table_func_decimal_type = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        takatori::decimal::triple value{0, 0, 0, 0};
        if (! args.empty() && args[0]) {
            value = args[0].to<takatori::decimal::triple>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<takatori::decimal::triple>, value}
        });
        takatori::decimal::triple value_plus_one{
            value.sign(),
            value.coefficient_high(),
            value.coefficient_low() + 1,
            value.exponent()
        };
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<takatori::decimal::triple>, value_plus_one}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_decimal_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::decimal>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::decimal>()},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_decimal_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_decimal_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    auto value = takatori::decimal::triple{1, 0, 12345, -2};
    auto value_plus_one = takatori::decimal::triple{
        value.sign(),
        value.coefficient_high(),
        value.coefficient_low() + 1,
        value.exponent()
    };

    // Note: T.C0 is DECIMAL(10,2) from table, but R.c1 is DECIMAL(*,*) from function
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal, kind::decimal>(
        std::tuple{meta::decimal_type(10, 2), meta::decimal_type()},
        std::forward_as_tuple(value, value),
        {false, false})), result[0]);
    EXPECT_EQ((mock::typed_nullable_record<kind::decimal, kind::decimal>(
        std::tuple{meta::decimal_type(10, 2), meta::decimal_type()},
        std::forward_as_tuple(value, value_plus_one),
        {false, false})), result[1]);
}

TEST_F(sql_apply_type_matrix_test, character_type) {
    execute_statement("CREATE TABLE T (C0 VARCHAR(100))");
    execute_statement("INSERT INTO T VALUES ('this_is_a_test_string_with_more_than_thirty_characters')");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12015;
    auto mock_table_func_character_type = [](
        evaluator_context& ctx,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        accessor::text value{};
        if (! args.empty() && args[0]) {
            value = args[0].to<accessor::text>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<accessor::text>, value}
        });
        std::string extended_string{value};
        extended_string += "X";
        accessor::text extended_value{ctx.resource(), extended_string};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<accessor::text>, extended_value}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_character_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::character>(t::varying)},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{
                std::make_shared<t::character>(t::varying)
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_character_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_character_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    memory::page_pool pool{};
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&pool);
    auto text1 = accessor::text{resource.get(), "this_is_a_test_string_with_more_than_thirty_characters"s};
    auto text2 = accessor::text{resource.get(), "this_is_a_test_string_with_more_than_thirty_charactersX"s};

    // Note: T.C0 is CHARACTER VARYING(100), R.c1 is CHARACTER VARYING(*)
    EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
        std::tuple{meta::character_type(true, 100), meta::character_type(true)},
        std::forward_as_tuple(text1, text1),
        {false, false})), result[0]);
    EXPECT_EQ((mock::typed_nullable_record<kind::character, kind::character>(
        std::tuple{meta::character_type(true, 100), meta::character_type(true)},
        std::forward_as_tuple(text1, text2),
        {false, false})), result[1]);
}

TEST_F(sql_apply_type_matrix_test, date_type) {
    execute_statement("CREATE TABLE T (C0 DATE)");
    execute_statement("INSERT INTO T VALUES (DATE'2024-01-15')");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12016;
    auto mock_table_func_date_type = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        takatori::datetime::date value{};
        if (! args.empty() && args[0]) {
            value = args[0].to<takatori::datetime::date>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<takatori::datetime::date>, value}
        });
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<takatori::datetime::date>, value + 1}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_date_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::date>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::date>()},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_date_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_date_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    auto date = takatori::datetime::date{2024, 1, 15};
    EXPECT_EQ((create_nullable_record<kind::date, kind::date>(date, date)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::date, kind::date>(date, date + 1)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, time_of_day_type) {
    execute_statement("CREATE TABLE T (C0 TIME)");
    execute_statement("INSERT INTO T VALUES (TIME'12:34:56')");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12017;
    auto mock_table_func_time_of_day_type = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        takatori::datetime::time_of_day value{};
        if (! args.empty() && args[0]) {
            value = args[0].to<takatori::datetime::time_of_day>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<takatori::datetime::time_of_day>, value}
        });
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<takatori::datetime::time_of_day>, value + std::chrono::seconds{1}}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_time_of_day_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::time_of_day>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::time_of_day>()},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_time_of_day_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_time_of_day_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    auto time = takatori::datetime::time_of_day{12, 34, 56};
    EXPECT_EQ((create_nullable_record<kind::time_of_day, kind::time_of_day>(time, time)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::time_of_day, kind::time_of_day>(time, time + std::chrono::seconds{1})), result[1]);
}

TEST_F(sql_apply_type_matrix_test, time_point_type) {
    execute_statement("CREATE TABLE T (C0 TIMESTAMP)");
    execute_statement("INSERT INTO T VALUES (TIMESTAMP'2024-01-15 12:34:56')");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12018;
    auto mock_table_func_time_point_type = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        takatori::datetime::time_point value{};
        if (! args.empty() && args[0]) {
            value = args[0].to<takatori::datetime::time_point>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<takatori::datetime::time_point>, value}
        });
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<takatori::datetime::time_point>, value + std::chrono::seconds{1}}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_time_point_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::time_point>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::time_point>()},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_time_point_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_time_point_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    auto timestamp = takatori::datetime::time_point{
        takatori::datetime::date{2024, 1, 15},
        takatori::datetime::time_of_day{12, 34, 56}
    };
    EXPECT_EQ((create_nullable_record<kind::time_point, kind::time_point>(timestamp, timestamp)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::time_point, kind::time_point>(timestamp, timestamp + std::chrono::seconds{1})), result[1]);
}

// boolean type not supported yet
TEST_F(sql_apply_type_matrix_test, DISABLED_boolean_type) {
    execute_statement("CREATE TABLE T (C0 BOOLEAN)");
    execute_statement("INSERT INTO T VALUES (TRUE)");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12019;
    auto mock_table_func_boolean_type = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        bool value = false;
        if (! args.empty() && args[0]) {
            value = args[0].to<bool>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<bool>, value}
        });
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<bool>, ! value}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_boolean_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::boolean>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::boolean>()},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_boolean_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_boolean_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::boolean, kind::boolean>(true, false)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::boolean, kind::boolean>(true, true)), result[1]);
}

TEST_F(sql_apply_type_matrix_test, binary_type) {
    execute_statement("CREATE TABLE T (C0 VARBINARY(100))");
    execute_statement("INSERT INTO T VALUES (X'0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF')");

    // Register inline function for this test
    constexpr std::size_t tvf_id = 12020;
    auto mock_table_func_binary_type = [](
        evaluator_context& ctx,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        accessor::binary value{};
        if (! args.empty() && args[0]) {
            value = args[0].to<accessor::binary>();
        }

        mock_any_sequence_stream::sequences_type sequences{};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<accessor::binary>, value}
        });
        std::string modified{value};
        modified.push_back('\xFF');
        accessor::binary extended_value{ctx.resource(), modified.data(), modified.size()};
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<accessor::binary>, extended_value}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    auto decl = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id, "mock_table_func_binary_type",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::octet>(t::varying)},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::octet>(t::varying)},
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );
    global::table_valued_function_repository().add(tvf_id, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_binary_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_binary_type(T.C0) AS R(c1)",
        result
    );

    // Cleanup
    if (decl) {
        global::regular_function_provider()->remove(*decl);
    }

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    memory::page_pool pool{};
    auto resource = std::make_shared<memory::lifo_paged_memory_resource>(&pool);
    std::string binary_data{"\x01\x23\x45\x67\x89\xAB\xCD\xEF\x01\x23\x45\x67\x89\xAB\xCD\xEF\x01\x23\x45\x67\x89\xAB\xCD\xEF\x01\x23\x45\x67\x89\xAB\xCD\xEF", 32};
    std::string binary_data_extended = binary_data + "\xFF";
    auto bin1 = accessor::binary{resource.get(), binary_data.data(), binary_data.size()};
    auto bin2 = accessor::binary{resource.get(), binary_data_extended.data(), binary_data_extended.size()};

    EXPECT_EQ((mock::typed_nullable_record<kind::octet, kind::octet>(
        std::tuple{meta::octet_type(true, 100), meta::octet_type(true)},
        std::forward_as_tuple(bin1, bin1),
        {false, false})), result[0]);
    EXPECT_EQ((mock::typed_nullable_record<kind::octet, kind::octet>(
        std::tuple{meta::octet_type(true, 100), meta::octet_type(true)},
        std::forward_as_tuple(bin1, bin2),
        {false, false})), result[1]);
}

}  // namespace jogasaki::testing
