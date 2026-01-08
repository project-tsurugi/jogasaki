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
#include "builtin_table_valued_functions.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

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
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/function/declaration.h>
#include <yugawara/function/function_feature.h>

#include <jogasaki/data/any.h>
#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/mock_any_sequence_stream.h>
#include <jogasaki/executor/function/builtin_table_valued_functions_id.h>
#include <jogasaki/executor/function/table_valued_function_info.h>
#include <jogasaki/executor/function/table_valued_function_kind.h>
#include <jogasaki/executor/function/table_valued_function_repository.h>

namespace jogasaki::executor::function {

namespace {

/**
 * @brief mock table-valued function that returns fixed rows.
 * @details returns 2 rows with columns (c1: INT4, c2: INT8).
 *          the first row contains (1, 100), the second row contains (2, 200).
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 INT4 argument as multiplier)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_fixed(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    std::int32_t multiplier = 1;
    if (! args.empty() && args[0]) {
        multiplier = args[0].to<std::int32_t>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};

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

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function that returns empty result.
 * @details used for testing OUTER APPLY behavior.
 * @param ctx the evaluator context
 * @param args the function arguments (ignored)
 * @return an empty stream
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_empty(
    evaluator_context& /* ctx */,
    sequence_view<data::any> /* args */
) {
    return std::make_unique<data::mock_any_sequence_stream>();
}

/**
 * @brief mock table-valued function that returns parameterized number of rows.
 * @details returns N rows where N is specified by the first argument.
 *          each row contains (i, i*10) where i is the row index (1-based).
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 INT4 argument as row count)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_generate(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    std::int32_t count = 0;
    if (! args.empty() && args[0]) {
        count = args[0].to<std::int32_t>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};

    for (std::int32_t i = 1; i <= count; ++i) {
        sequences.emplace_back(data::any_sequence::storage_type{
            data::any{std::in_place_type<std::int32_t>, i},
            data::any{std::in_place_type<std::int64_t>, static_cast<std::int64_t>(i) * 10}
        });
    }

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function that returns rows with three columns.
 * @details returns 2 rows with columns (c1: INT4, c2: INT8, c3: INT4).
 *          the first row contains (1, 100, 1000), the second row contains (2, 200, 2000).
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 INT4 argument as multiplier)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_three_columns(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    std::int32_t multiplier = 1;
    if (! args.empty() && args[0]) {
        multiplier = args[0].to<std::int32_t>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};

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

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: INT4.
 * @details takes INT4 argument and returns 2 rows with INT4 column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 INT4 argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_int4_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    std::int32_t value = 0;
    if (! args.empty() && args[0]) {
        value = args[0].to<std::int32_t>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<std::int32_t>, value}
    });
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<std::int32_t>, value + 1}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: INT8.
 * @details takes INT8 argument and returns 2 rows with INT8 column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 INT8 argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_int8_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    std::int64_t value = 0;
    if (! args.empty() && args[0]) {
        value = args[0].to<std::int64_t>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<std::int64_t>, value}
    });
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<std::int64_t>, value + 1}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: FLOAT4.
 * @details takes FLOAT4 argument and returns 2 rows with FLOAT4 column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 FLOAT4 argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_float4_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    float value = 0.0F;
    if (! args.empty() && args[0]) {
        value = args[0].to<float>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<float>, value}
    });
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<float>, value + 1.0F}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: FLOAT8.
 * @details takes FLOAT8 argument and returns 2 rows with FLOAT8 column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 FLOAT8 argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_float8_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    double value = 0.0;
    if (! args.empty() && args[0]) {
        value = args[0].to<double>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<double>, value}
    });
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<double>, value + 1.0}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: DECIMAL.
 * @details takes DECIMAL argument and returns 2 rows with DECIMAL column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 DECIMAL argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_decimal_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    takatori::decimal::triple value{0, 0, 0, 0};
    if (! args.empty() && args[0]) {
        value = args[0].to<takatori::decimal::triple>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<takatori::decimal::triple>, value}
    });
    // create a second value (with different coefficient)
    takatori::decimal::triple value_plus_one{
        value.sign(),
        value.coefficient_high(),
        value.coefficient_low() + 1,
        value.exponent()
    };
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<takatori::decimal::triple>, value_plus_one}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: CHARACTER.
 * @details takes CHARACTER argument and returns 2 rows with CHARACTER column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 CHARACTER argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_character_type(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
    accessor::text value{};
    if (! args.empty() && args[0]) {
        value = args[0].to<accessor::text>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<accessor::text>, value}
    });
    std::string extended_string{value};
    extended_string += "X";
    accessor::text extended_value{ctx.resource(), extended_string};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<accessor::text>, extended_value}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: DATE.
 * @details takes DATE argument and returns 2 rows with DATE column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 DATE argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_date_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    takatori::datetime::date value{};
    if (! args.empty() && args[0]) {
        value = args[0].to<takatori::datetime::date>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<takatori::datetime::date>, value}
    });
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<takatori::datetime::date>, value + 1}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: TIME_OF_DAY.
 * @details takes TIME_OF_DAY argument and returns 2 rows with TIME_OF_DAY column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 TIME_OF_DAY argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_time_of_day_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    takatori::datetime::time_of_day value{};
    if (! args.empty() && args[0]) {
        value = args[0].to<takatori::datetime::time_of_day>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<takatori::datetime::time_of_day>, value}
    });
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<takatori::datetime::time_of_day>, value + std::chrono::seconds{1}}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: TIME_POINT.
 * @details takes TIME_POINT argument and returns 2 rows with TIME_POINT column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 TIME_POINT argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_time_point_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    takatori::datetime::time_point value{};
    if (! args.empty() && args[0]) {
        value = args[0].to<takatori::datetime::time_point>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<takatori::datetime::time_point>, value}
    });
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<takatori::datetime::time_point>, value + std::chrono::seconds{1}}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: BOOLEAN.
 * @details takes BOOLEAN argument and returns 2 rows with BOOLEAN column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 BOOLEAN argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_boolean_type(
    evaluator_context& /* ctx */,
    sequence_view<data::any> args
) {
    bool value = false;
    if (! args.empty() && args[0]) {
        value = args[0].to<bool>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<bool>, value}
    });
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<bool>, ! value}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

/**
 * @brief mock table-valued function for type matrix testing: BINARY.
 * @details takes BINARY argument and returns 2 rows with BINARY column.
 * @param ctx the evaluator context
 * @param args the function arguments (expects 1 BINARY argument)
 * @return the stream of result rows
 */
std::unique_ptr<data::any_sequence_stream> mock_table_func_binary_type(
    evaluator_context& ctx,
    sequence_view<data::any> args
) {
    accessor::binary value{};
    if (! args.empty() && args[0]) {
        value = args[0].to<accessor::binary>();
    }

    data::mock_any_sequence_stream::sequences_type sequences{};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<accessor::binary>, value}
    });
    std::string modified{value};
    modified.push_back('\xFF');
    accessor::binary extended_value{ctx.resource(), modified.data(), modified.size()};
    sequences.emplace_back(data::any_sequence::storage_type{
        data::any{std::in_place_type<accessor::binary>, extended_value}
    });

    return std::make_unique<data::mock_any_sequence_stream>(std::move(sequences));
}

}  // namespace

void add_builtin_table_valued_functions(
    yugawara::function::configurable_provider& functions,
    table_valued_function_repository& repo
) {
    namespace ttype = ::takatori::type;

    // first register function declarations to yugawara for SQL compilation
    // mock_table_func_fixed: (multiplier: INT4) -> TABLE(c1: INT4, c2: INT8)
    functions.add(yugawara::function::declaration{
        tvf_id_12000,
        "mock_table_func_fixed",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::int4>()},
            {"c2", std::make_shared<ttype::int8>()},
        }),
        {
            std::make_shared<ttype::int4>(),  // multiplier parameter
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    // mock_table_func_empty: () -> TABLE(c1: INT4, c2: INT8)
    functions.add(yugawara::function::declaration{
        tvf_id_12001,
        "mock_table_func_empty",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::int4>()},
            {"c2", std::make_shared<ttype::int8>()},
        }),
        {},  // no parameters
        {yugawara::function::function_feature::table_valued_function},
    });

    // mock_table_func_generate: (count: INT4) -> TABLE(c1: INT4, c2: INT8)
    functions.add(yugawara::function::declaration{
        tvf_id_12002,
        "mock_table_func_generate",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::int4>()},
            {"c2", std::make_shared<ttype::int8>()},
        }),
        {
            std::make_shared<ttype::int4>(),  // count parameter
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    // mock_table_func_three_columns: (multiplier: INT4) -> TABLE(c1: INT4, c2: INT8, c3: INT4)
    functions.add(yugawara::function::declaration{
        tvf_id_12003,
        "mock_table_func_three_columns",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::int4>()},
            {"c2", std::make_shared<ttype::int8>()},
            {"c3", std::make_shared<ttype::int4>()},
        }),
        {
            std::make_shared<ttype::int4>(),  // multiplier parameter
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    // then register the execution information to our repository
    repo.add(
        tvf_id_12000,
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
    repo.add(
        tvf_id_12001,
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
    repo.add(
        tvf_id_12002,
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

    // mock_table_func_three_columns: (multiplier: INT4) -> TABLE(c1: INT4, c2: INT8, c3: INT4)
    repo.add(
        tvf_id_12003,
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

    // type matrix test functions
    // mock_table_func_int4_type: (value: INT4) -> TABLE(c1: INT4)
    functions.add(yugawara::function::declaration{
        tvf_id_12010,
        "mock_table_func_int4_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::int4>()},
        }),
        {
            std::make_shared<ttype::int4>(),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12010,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_int4_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_int8_type: (value: INT8) -> TABLE(c1: INT8)
    functions.add(yugawara::function::declaration{
        tvf_id_12011,
        "mock_table_func_int8_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::int8>()},
        }),
        {
            std::make_shared<ttype::int8>(),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12011,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_int8_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_float4_type: (value: FLOAT4) -> TABLE(c1: FLOAT4)
    functions.add(yugawara::function::declaration{
        tvf_id_12012,
        "mock_table_func_float4_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::float4>()},
        }),
        {
            std::make_shared<ttype::float4>(),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12012,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_float4_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_float8_type: (value: FLOAT8) -> TABLE(c1: FLOAT8)
    functions.add(yugawara::function::declaration{
        tvf_id_12013,
        "mock_table_func_float8_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::float8>()},
        }),
        {
            std::make_shared<ttype::float8>(),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12013,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_float8_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_decimal_type: (value: DECIMAL) -> TABLE(c1: DECIMAL)
    functions.add(yugawara::function::declaration{
        tvf_id_12014,
        "mock_table_func_decimal_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::decimal>()},
        }),
        {
            std::make_shared<ttype::decimal>(),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12014,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_decimal_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_character_type: (value: CHARACTER(*)) -> TABLE(c1: CHARACTER(*))
    functions.add(yugawara::function::declaration{
        tvf_id_12015,
        "mock_table_func_character_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::character>(ttype::varying)},
        }),
        {
            std::make_shared<ttype::character>(ttype::varying),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12015,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_character_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_date_type: (value: DATE) -> TABLE(c1: DATE)
    functions.add(yugawara::function::declaration{
        tvf_id_12016,
        "mock_table_func_date_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::date>()},
        }),
        {
            std::make_shared<ttype::date>(),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12016,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_date_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_time_of_day_type: (value: TIME_OF_DAY) -> TABLE(c1: TIME_OF_DAY)
    functions.add(yugawara::function::declaration{
        tvf_id_12017,
        "mock_table_func_time_of_day_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::time_of_day>()},
        }),
        {
            std::make_shared<ttype::time_of_day>(),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12017,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_time_of_day_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_time_point_type: (value: TIME_POINT) -> TABLE(c1: TIME_POINT)
    functions.add(yugawara::function::declaration{
        tvf_id_12018,
        "mock_table_func_time_point_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::time_point>()},
        }),
        {
            std::make_shared<ttype::time_point>(),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12018,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_time_point_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_boolean_type: (value: BOOLEAN) -> TABLE(c1: BOOLEAN)
    functions.add(yugawara::function::declaration{
        tvf_id_12019,
        "mock_table_func_boolean_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::boolean>()},
        }),
        {
            std::make_shared<ttype::boolean>(),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12019,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_boolean_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );

    // mock_table_func_binary_type: (value: BINARY(*)) -> TABLE(c1: BINARY(*))
    functions.add(yugawara::function::declaration{
        tvf_id_12020,
        "mock_table_func_binary_type",
        std::make_shared<ttype::table>(std::initializer_list<ttype::table::column_type>{
            {"c1", std::make_shared<ttype::octet>(ttype::varying)},
        }),
        {
            std::make_shared<ttype::octet>(ttype::varying),
        },
        {yugawara::function::function_feature::table_valued_function},
    });

    repo.add(
        tvf_id_12020,
        std::make_shared<table_valued_function_info>(
            table_valued_function_kind::builtin,
            mock_table_func_binary_type,
            1,
            table_valued_function_info::columns_type{
                table_valued_function_column{"c1"}
            }
        )
    );
}

}  // namespace jogasaki::executor::function
