/*
 * Copyright 2018-2025 Project Tsurugi.
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

#include <yugawara/aggregate/declaration.h>

namespace jogasaki::executor::function {

constexpr static std::size_t minimum_scalar_function_id = 1000;

/**
 * @brief the sequential number pool for the build-in aggregate functions
 * @details this is to avoid the collision of the function ids and to clarify the function id range in use.
 * @warning it's possible the function ids are made durable (e.g. scalar function in the CREATE TABLE DEFAULT clause)
 * so do not change the existing entries or re-use the numbers that are already used.
 * When a function id gets deprecated and unused any more, simply leave them as it it or delete the entry by assigning
 * the number to the next entry to keep the gap.
 * When you add a new function, increment the last entry and append it add the end of the list.
 * The function id is simply the unique identifier of the function, so the order is not significant. Overloaded functions
 * do not need to have adjacent ids.
 */
enum scalar_function_id : std::size_t {
    id_11000 = yugawara::function::declaration::minimum_builtin_function_id + minimum_scalar_function_id,
    id_11001,
    id_11002,
    id_11003,
    id_11004,
    id_11005,
    id_11006,
    id_11007,
    id_11008,
    id_11009
};

static_assert(scalar_function_id::id_11000 == 11'000);
static_assert(scalar_function_id::id_11005 == 11'005);

}  // namespace jogasaki::executor::function
