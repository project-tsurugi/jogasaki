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
};

}  // namespace jogasaki::executor::function
