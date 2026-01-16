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

#include <cstdint>
#include <initializer_list>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>

#include <takatori/datetime/date.h>
#include <takatori/type/character.h>
#include <takatori/type/date.h>
#include <takatori/type/primitive.h>
#include <takatori/type/table.h>
#include <takatori/type/varying.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/function/configurable_provider.h>
#include <yugawara/function/declaration.h>

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

/**
 * @brief mock table-valued function that returns fixed rows.
 * @details returns 2 rows with columns (c1: INT4, c2: INT8).
 *          the first row contains (1 * multiplier, 100 * multiplier),
 *          the second row contains (2 * multiplier, 200 * multiplier).
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_fixed(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    std::int32_t multiplier = 1;
    if (! args.empty() && args[0]) {
        multiplier = args[0].to<std::int32_t>();
    }

    mock_any_sequence_stream::sequences_type sequences{};

    // row 1: (1 * multiplier, 100 * multiplier)
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<std::int32_t>, 1 * multiplier},
        data::any{std::in_place_type<std::int64_t>, static_cast<std::int64_t>(100) * multiplier}
    });

    // row 2: (2 * multiplier, 200 * multiplier)
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<std::int32_t>, 2 * multiplier},
        data::any{std::in_place_type<std::int64_t>, static_cast<std::int64_t>(200) * multiplier}
    });

    return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function that returns empty result.
 * @details used for testing OUTER APPLY behavior.
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_empty(
    evaluator_context& /* ctx */,
    sequence_view<data::any> /* args */
) {
    return std::make_unique<mock_any_sequence_stream>();
}

/**
 * @brief mock table-valued function that returns parameterized number of rows.
 * @details returns N rows where N is specified by the first argument.
 *          each row contains (i, i*10) where i is the row index (1-based).
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_generate(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    std::int32_t count = 0;
    if (! args.empty() && args[0]) {
        count = args[0].to<std::int32_t>();
    }

    mock_any_sequence_stream::sequences_type sequences{};

    for (std::int32_t i = 1; i <= count; ++i) {
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int32_t>, i},
            data::any{std::in_place_type<std::int64_t>, static_cast<std::int64_t>(i) * 10}
        });
    }

    return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function that returns rows with int4 type.
 * @details used for type matrix tests. Returns 2 rows with (value, value+1).
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_int4_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
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
}

/**
 * @brief mock table-valued function that returns rows with character type.
 * @details used for type matrix tests. Returns 2 rows with (value, value+"X").
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_character_type(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
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
}

/**
 * @brief mock table-valued function that returns rows with date type.
 * @details used for type matrix tests. Returns 2 rows with (value, value+1).
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_date_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
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
}

constexpr std::size_t tvf_id_fixed = 12000;
constexpr std::size_t tvf_id_empty = 12001;
constexpr std::size_t tvf_id_generate = 12002;
constexpr std::size_t tvf_id_int4 = 12010;
constexpr std::size_t tvf_id_character = 12015;
constexpr std::size_t tvf_id_date = 12016;

/**
 * @brief holds declarations for registered mock functions to enable cleanup.
 */
struct mock_function_declarations {
    std::shared_ptr<yugawara::function::declaration> decl_fixed{};
    std::shared_ptr<yugawara::function::declaration> decl_empty{};
    std::shared_ptr<yugawara::function::declaration> decl_generate{};
    std::shared_ptr<yugawara::function::declaration> decl_int4{};
    std::shared_ptr<yugawara::function::declaration> decl_character{};
    std::shared_ptr<yugawara::function::declaration> decl_date{};
};

mock_function_declarations register_mock_table_valued_functions(
    yugawara::function::configurable_provider& functions,
    table_valued_function_repository& repo
) {
    mock_function_declarations decls{};

    // mock_table_func_fixed: (multiplier: INT4) -> TABLE(c1: INT4, c2: INT8)
    decls.decl_fixed = functions.add(std::make_shared<yugawara::function::declaration>(
        tvf_id_fixed,
        "mock_table_func_fixed",
        std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
            {"c1", std::make_shared<t::int4>()},
            {"c2", std::make_shared<t::int8>()},
        }),
        std::vector<std::shared_ptr<takatori::type::data const>>{
            std::make_shared<t::int4>(),  // multiplier parameter
        },
        yugawara::function::declaration::feature_set_type{
            yugawara::function::function_feature::table_valued_function
        }
    ));

    repo.add(
        tvf_id_fixed,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_fixed,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"},
                table_valued_function_column{"c2"}
            }
        )
    );

    // mock_table_func_empty: () -> TABLE(c1: INT4, c2: INT8)
    decls.decl_empty = functions.add(std::make_shared<yugawara::function::declaration>(
        tvf_id_empty,
        "mock_table_func_empty",
        std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
            {"c1", std::make_shared<t::int4>()},
            {"c2", std::make_shared<t::int8>()},
        }),
        std::vector<std::shared_ptr<takatori::type::data const>>{},  // no parameters
        yugawara::function::declaration::feature_set_type{
            yugawara::function::function_feature::table_valued_function
        }
    ));

    repo.add(
        tvf_id_empty,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_empty,
            0,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"},
                table_valued_function_column{"c2"}
            }
        )
    );

    // mock_table_func_generate: (count: INT4) -> TABLE(c1: INT4, c2: INT8)
    decls.decl_generate = functions.add(std::make_shared<yugawara::function::declaration>(
        tvf_id_generate,
        "mock_table_func_generate",
        std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
            {"c1", std::make_shared<t::int4>()},
            {"c2", std::make_shared<t::int8>()},
        }),
        std::vector<std::shared_ptr<takatori::type::data const>>{
            std::make_shared<t::int4>(),  // count parameter
        },
        yugawara::function::declaration::feature_set_type{
            yugawara::function::function_feature::table_valued_function
        }
    ));

    repo.add(
        tvf_id_generate,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_generate,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"},
                table_valued_function_column{"c2"}
            }
        )
    );

    // mock_table_func_int4_type: (value: INT4) -> TABLE(c1: INT4)
    decls.decl_int4 = functions.add(std::make_shared<yugawara::function::declaration>(
        tvf_id_int4, "mock_table_func_int4_type",
        std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
            {"c1", std::make_shared<t::int4>()},
        }),
        std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::int4>()},
        yugawara::function::declaration::feature_set_type{
            yugawara::function::function_feature::table_valued_function
        }
    ));
    repo.add(tvf_id_int4, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_int4_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    // mock_table_func_character_type: (value: VARCHAR) -> TABLE(c1: VARCHAR)
    decls.decl_character = functions.add(std::make_shared<yugawara::function::declaration>(
        tvf_id_character, "mock_table_func_character_type",
        std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
            {"c1", std::make_shared<t::character>(t::varying)},
        }),
        std::vector<std::shared_ptr<takatori::type::data const>>{
            std::make_shared<t::character>(t::varying)
        },
        yugawara::function::declaration::feature_set_type{
            yugawara::function::function_feature::table_valued_function
        }
    ));
    repo.add(tvf_id_character, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_character_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    // mock_table_func_date_type: (value: DATE) -> TABLE(c1: DATE)
    decls.decl_date = functions.add(std::make_shared<yugawara::function::declaration>(
        tvf_id_date, "mock_table_func_date_type",
        std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
            {"c1", std::make_shared<t::date>()},
        }),
        std::vector<std::shared_ptr<takatori::type::data const>>{std::make_shared<t::date>()},
        yugawara::function::declaration::feature_set_type{
            yugawara::function::function_feature::table_valued_function
        }
    ));
    repo.add(tvf_id_date, std::make_shared<table_valued_function_info>(
        table_valued_function_kind::builtin, mock_table_func_date_type, 1,
        table_valued_function_info::columns_type{table_valued_function_column{"c1"}}
    ));

    return decls;
}

void unregister_mock_table_valued_functions(
    yugawara::function::configurable_provider& functions,
    table_valued_function_repository& repo,
    mock_function_declarations const& decls
) {
    repo.clear();

    if (decls.decl_fixed) {
        functions.remove(*decls.decl_fixed);
    }
    if (decls.decl_empty) {
        functions.remove(*decls.decl_empty);
    }
    if (decls.decl_generate) {
        functions.remove(*decls.decl_generate);
    }
    if (decls.decl_int4) {
        functions.remove(*decls.decl_int4);
    }
    if (decls.decl_character) {
        functions.remove(*decls.decl_character);
    }
    if (decls.decl_date) {
        functions.remove(*decls.decl_date);
    }
}

}  // namespace

/**
 * @brief test for APPLY operator (CROSS APPLY / OUTER APPLY)
 * @details this test uses mock table-valued functions to test the APPLY operator.
 */
class sql_apply_test :
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

        // register mock table-valued functions
        decls_ = register_mock_table_valued_functions(
            *global::regular_function_provider(),
            global::table_valued_function_repository()
        );
    }

    void TearDown() override {
        unregister_mock_table_valued_functions(
            *global::regular_function_provider(),
            global::table_valued_function_repository(),
            decls_
        );
        db_teardown();
    }

private:
    mock_function_declarations decls_{};
};

TEST_F(sql_apply_test, cross_apply_basic) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    // CROSS APPLY with mock_table_func_fixed
    // mock_table_func_fixed(multiplier) returns:
    //   (1 * multiplier, 100 * multiplier)
    //   (2 * multiplier, 200 * multiplier)
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, T.C1, R.c1, R.c2 "
        "FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c1, c2)",
        result
    );

    // expected output:
    // T.C0=1, T.C1=100 × 2 rows from function = 2 rows
    // T.C0=2, T.C1=200 × 2 rows from function = 2 rows
    // total: 4 rows
    ASSERT_EQ(4, result.size());

    std::sort(result.begin(), result.end());

    // first input row (1, 100) × function output (1*1, 100*1) and (2*1, 200*1)
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(1, 100, 1, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(1, 100, 2, 200)), result[1]);

    // second input row (2, 200) × function output (1*2, 100*2) and (2*2, 200*2)
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(2, 200, 2, 200)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(2, 200, 4, 400)), result[3]);
}

TEST_F(sql_apply_test, cross_apply_empty_input) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // no data in table T
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c1, c2)",
        result
    );

    // expected: empty output
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_apply_test, cross_apply_empty_right) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // mock_table_func_empty() returns empty result
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_empty() AS R(c1, c2)",
        result
    );

    // expected: empty output (CROSS APPLY eliminates rows when right side is empty)
    ASSERT_EQ(0, result.size());
}

TEST_F(sql_apply_test, outer_apply_empty_right) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    // mock_table_func_empty() returns empty result
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, T.C1, R.c1, R.c2 "
        "FROM T OUTER APPLY mock_table_func_empty() AS R(c1, c2)",
        result
    );

    // expected: 2 rows with NULL for R.c1 and R.c2
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(
        std::tuple{1, 100, 0, 0}, {false, false, true, true})), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(
        std::tuple{2, 200, 0, 0}, {false, false, true, true})), result[1]);
}

TEST_F(sql_apply_test, cross_apply_multiple_rows) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // mock_table_func_generate(count) returns N rows: (1, 10), (2, 20), ..., (N, N*10)
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1, R.c2 FROM T CROSS APPLY mock_table_func_generate(3::int) AS R(c1, c2)",
        result
    );

    // expected: 1 input row × 3 rows from function = 3 rows
    ASSERT_EQ(3, result.size());

    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 2, 20)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 3, 30)), result[2]);
}

TEST_F(sql_apply_test, cross_apply_with_where) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");
    execute_statement("INSERT INTO T VALUES (3, 300)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c1, c2) "
        "WHERE T.C0 = 2",
        result
    );

    // expected: only rows for T.C0 = 2
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 2)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(2, 4)), result[1]);
}

TEST_F(sql_apply_test, outer_apply_basic) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, T.C1, R.c1, R.c2 "
        "FROM T OUTER APPLY mock_table_func_fixed(T.C0) AS R(c1, c2)",
        result
    );

    // expected: same as CROSS APPLY when right side is not empty
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(1, 100, 1, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8, kind::int4, kind::int8>(1, 100, 2, 200)), result[1]);
}

TEST_F(sql_apply_test, cross_apply_parameter_from_function) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (5, 100)");

    // use function result as parameter to another APPLY
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_generate(T.C0) AS R(c1, c2)",
        result
    );

    // expected: 1 input row (C0=5) × 5 rows from function = 5 rows
    ASSERT_EQ(5, result.size());

    std::sort(result.begin(), result.end());
    for (int i = 0; i < 5; ++i) {
        EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(5, i + 1)), result[i]);
    }
}

TEST_F(sql_apply_test, cross_apply_twice) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");
    execute_statement("INSERT INTO T VALUES (2, 200)");

    // CROSS APPLY twice: first APPLY generates rows, second APPLY uses those rows
    // mock_table_func_fixed(multiplier) returns (multiplier, 100*multiplier), (2*multiplier, 200*multiplier)
    // Then mock_table_func_generate(count) returns N rows: (1, 10), (2, 20), ..., (N, N*10)
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R1.c1, R2.c1, R2.c2 "
        "FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R1 "
        "CROSS APPLY mock_table_func_generate(R1.c1) AS R2(c1, c2)",
        result
    );

    // expected: complex nested result
    // For T.C0=1: R1 has (1,100) and (2,200), then for each R1.c1, generate R1.c1 rows
    // For (1,100): generate 1 row -> (1,10)
    // For (2,200): generate 2 rows -> (1,10), (2,20)
    // For T.C0=2: R1 has (2,200) and (4,400), then for each R1.c1, generate R1.c1 rows
    // For (2,200): generate 2 rows -> (1,10), (2,20)
    // For (4,400): generate 4 rows -> (1,10), (2,20), (3,30), (4,40)
    // Total: 1 + 2 + 2 + 4 = 9 rows
    ASSERT_EQ(9, result.size());

    std::sort(result.begin(), result.end());

    // Expected rows after sorting
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(1, 1, 1, 10)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(1, 2, 1, 10)), result[1]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(1, 2, 2, 20)), result[2]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 2, 1, 10)), result[3]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 2, 2, 20)), result[4]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 4, 1, 10)), result[5]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 4, 2, 20)), result[6]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 4, 3, 30)), result[7]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int4, kind::int8>(2, 4, 4, 40)), result[8]);
}

TEST_F(sql_apply_test, cross_apply_column_alias) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // Function returns (c1, c2), but SQL specifies AS R(c2, c1) - column names are swapped
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c2, R.c1 "
        "FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c2, c1)",
        result
    );

    // mock_table_func_fixed(1) returns (1, 100), (2, 200)
    // With AS R(c2, c1), R.c2 gets function's c1, R.c1 gets function's c2
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());

    // First row: T.C0=1, R.c2=function.c1=1, R.c1=function.c2=100
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 100)), result[0]);
    // Second row: T.C0=1, R.c2=function.c1=2, R.c1=function.c2=200
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 2, 200)), result[1]);
}

TEST_F(sql_apply_test, cross_apply_column_alias_different_names) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // Function returns (c1, c2), but SQL specifies AS R(c10, c20) - completely different names
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c10, R.c20 "
        "FROM T CROSS APPLY mock_table_func_fixed(T.C0) AS R(c10, c20)",
        result
    );

    // mock_table_func_fixed(1) returns (1, 100), (2, 200)
    // With AS R(c10, c20), R.c10 gets function's c1, R.c20 gets function's c2
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());

    // First row: T.C0=1, R.c10=function.c1=1, R.c20=function.c2=100
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 1, 100)), result[0]);
    // Second row: T.C0=1, R.c10=function.c1=2, R.c20=function.c2=200
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::int8>(1, 2, 200)), result[1]);
}

TEST_F(sql_apply_test, cross_apply_unused_columns) {
    execute_statement("CREATE TABLE T (C0 INT PRIMARY KEY, C1 BIGINT)");
    // insert test data
    execute_statement("INSERT INTO T VALUES (1, 100)");

    // Register mock_table_func_three_columns inline for this test
    // Returns 2 rows with columns (c1: INT4, c2: INT8, c3: INT4)
    constexpr std::size_t tvf_id_three_columns = 12003;

    auto mock_table_func_three_columns = [](
        evaluator_context& /* ctx */,
        sequence_view<data::any> args
    ) -> std::unique_ptr<data::any_sequence_stream> {
        std::int32_t multiplier = 1;
        if (! args.empty() && args[0]) {
            multiplier = args[0].to<std::int32_t>();
        }

        mock_any_sequence_stream::sequences_type sequences{};

        // row 1: (1 * multiplier, 100 * multiplier, 1000 * multiplier)
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int32_t>, 1 * multiplier},
            data::any{std::in_place_type<std::int64_t>, static_cast<std::int64_t>(100) * multiplier},
            data::any{std::in_place_type<std::int32_t>, 1000 * multiplier}
        });

        // row 2: (2 * multiplier, 200 * multiplier, 2000 * multiplier)
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int32_t>, 2 * multiplier},
            data::any{std::in_place_type<std::int64_t>, static_cast<std::int64_t>(200) * multiplier},
            data::any{std::in_place_type<std::int32_t>, 2000 * multiplier}
        });

        return std::make_unique<mock_any_sequence_stream>(std::move(sequences));
    };

    // Register the function
    auto decl_three_columns = global::regular_function_provider()->add(
        std::make_shared<yugawara::function::declaration>(
            tvf_id_three_columns,
            "mock_table_func_three_columns",
            std::make_shared<t::table>(std::initializer_list<t::table::column_type>{
                {"c1", std::make_shared<t::int4>()},
                {"c2", std::make_shared<t::int8>()},
                {"c3", std::make_shared<t::int4>()},
            }),
            std::vector<std::shared_ptr<takatori::type::data const>>{
                std::make_shared<t::int4>(),  // multiplier parameter
            },
            yugawara::function::declaration::feature_set_type{
                yugawara::function::function_feature::table_valued_function
            }
        )
    );

    global::table_valued_function_repository().add(
        tvf_id_three_columns,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_three_columns,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"},
                table_valued_function_column{"c2"},
                table_valued_function_column{"c3"}
            }
        )
    );

    // Function returns 3 columns (c1, c2, c3), but SQL only uses c2 - other columns should be discarded
    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c2 "
        "FROM T CROSS APPLY mock_table_func_three_columns(T.C0) AS R(c1, c2, c3)",
        result
    );

    // Unregister the function (note: repository entries will be cleared in TearDown via clear())
    if (decl_three_columns) {
        global::regular_function_provider()->remove(*decl_three_columns);
    }

    // mock_table_func_three_columns(1) returns (1, 100, 1000), (2, 200, 2000)
    // Only c2 is used in SELECT, c1 and c3 are discarded
    ASSERT_EQ(2, result.size());

    std::sort(result.begin(), result.end());

    // First row: T.C0=1, R.c2=100
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(1, 100)), result[0]);
    // Second row: T.C0=1, R.c2=200
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int8>(1, 200)), result[1]);
}

TEST_F(sql_apply_test, multiple_types_in_single_query) {
    execute_statement("CREATE TABLE T (C0 INT, C1 VARCHAR(100), C2 DATE)");
    execute_statement("INSERT INTO T VALUES (42, 'hello', DATE'2024-01-01')");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R1.c1, R2.c1, R3.c1 "
        "FROM T "
        "CROSS APPLY mock_table_func_int4_type(T.C0) AS R1(c1) "
        "CROSS APPLY mock_table_func_character_type(T.C1) AS R2(c1) "
        "CROSS APPLY mock_table_func_date_type(T.C2) AS R3(c1)",
        result
    );

    // 1 input row × 2 rows from R1 × 2 rows from R2 × 2 rows from R3 = 8 rows
    ASSERT_EQ(8, result.size());

    std::sort(result.begin(), result.end());

    auto date = takatori::datetime::date{2024, 1, 1};

    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::character, kind::date>(
        42, 42, accessor::text{"hello"s}, date)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4, kind::character, kind::date>(
        42, 43, accessor::text{"helloX"s}, date + 1)), result[7]);
}

TEST_F(sql_apply_test, outer_apply_with_various_types) {
    execute_statement("CREATE TABLE T (C0 INT)");
    execute_statement("INSERT INTO T VALUES (100)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T OUTER APPLY mock_table_func_int4_type(T.C0) AS R(c1)",
        result
    );

    // same as CROSS APPLY when right side is not empty
    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(100, 100)), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(100, 101)), result[1]);
}

TEST_F(sql_apply_test, null_values) {
    execute_statement("CREATE TABLE T (C0 INT)");
    execute_statement("INSERT INTO T VALUES (NULL)");

    std::vector<mock::basic_record> result{};
    execute_query(
        "SELECT T.C0, R.c1 FROM T CROSS APPLY mock_table_func_int4_type(T.C0) AS R(c1)",
        result
    );

    ASSERT_EQ(2, result.size());
    std::sort(result.begin(), result.end());

    // null value is passed as 0 to the function
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(
        std::tuple{0, 0}, {true, false})), result[0]);
    EXPECT_EQ((create_nullable_record<kind::int4, kind::int4>(
        std::tuple{0, 1}, {true, false})), result[1]);
}

}  // namespace jogasaki::testing
