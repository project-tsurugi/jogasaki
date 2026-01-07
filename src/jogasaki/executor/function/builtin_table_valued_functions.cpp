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

#include <memory>

#include <takatori/type/primitive.h>
#include <takatori/type/table.h>
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
}

}  // namespace jogasaki::executor::function
