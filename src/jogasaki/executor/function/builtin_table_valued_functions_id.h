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
#pragma once

#include <cstddef>

#include <yugawara/function/declaration.h>

namespace jogasaki::executor::function {

/**
 * @brief the offset for table-valued function ids.
 * @details this separates table-valued function ids from scalar and aggregate function ids.
 */
constexpr static std::size_t minimum_table_valued_function_id = 2000;

/**
 * @brief the sequential number pool for the built-in table-valued functions.
 * @details this is to avoid the collision of the function ids and to clarify the function id range in use.
 * @warning it's possible the function ids are made durable, so do not change the existing entries
 * or re-use the numbers that are already used.
 * When a function id gets deprecated and unused any more, simply leave them as it is or delete the entry
 * by assigning the number to the next entry to keep the gap.
 * When you add a new function, increment the last entry and append it at the end of the list.
 * The function id is simply the unique identifier of the function, so the order is not significant.
 */
enum table_valued_function_id : std::size_t {
    /**
     * @brief mock table-valued function for testing.
     * @details returns a fixed set of rows for testing purposes.
     */
    tvf_id_12000 = yugawara::function::declaration::minimum_builtin_function_id + minimum_table_valued_function_id,

    /**
     * @brief mock table-valued function that returns empty result.
     * @details used for testing OUTER APPLY behavior.
     */
    tvf_id_12001,

    /**
     * @brief mock table-valued function that returns multiple rows.
     * @details returns a parameterized number of rows.
     */
    tvf_id_12002,

    /**
     * @brief mock table-valued function with three columns.
     * @details returns rows with three columns for testing column discarding.
     */
    tvf_id_12003,

    /**
     * @brief mock table-valued functions for type matrix testing.
     * @details each function takes one argument of specific type and returns a table with one column of the same type.
     */
    tvf_id_12010,  // mock_table_func_int4_type
    tvf_id_12011,  // mock_table_func_int8_type
    tvf_id_12012,  // mock_table_func_float4_type
    tvf_id_12013,  // mock_table_func_float8_type
    tvf_id_12014,  // mock_table_func_decimal_type
    tvf_id_12015,  // mock_table_func_character_type
    tvf_id_12016,  // mock_table_func_date_type
    tvf_id_12017,  // mock_table_func_time_of_day_type
    tvf_id_12018,  // mock_table_func_time_point_type
    tvf_id_12019,  // mock_table_func_boolean_type
    tvf_id_12020,  // mock_table_func_binary_type
};

}  // namespace jogasaki::executor::function
