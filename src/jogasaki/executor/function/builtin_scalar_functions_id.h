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
 * @brief the sequential number pool for the build-in scalar functions
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
    id_11009,
    id_11010,
    id_11011,
    id_11012,
    id_11013,
    id_11014,
    id_11015,
    id_11016,
    id_11017,
    id_11018,
    id_11019,
    id_11020,
    id_11021,
    id_11022,
    id_11023,
    id_11024,
    id_11025,
    id_11026,
    id_11027,
    id_11028,
    id_11029,
    id_11030,
    id_11031,
    id_11032,
    id_11033,
    id_11034,
    id_11035,
    id_11036,
    id_11037,
    id_11038,
    id_11039,
    id_11040,
    id_11041,
    id_11042,
    id_11043,
    id_11044,
    id_11045,
    id_11046,
    id_11047,
    id_11048,
    id_11049,
    id_11050,
    id_11051,
    id_11052,
    id_11053,
    id_11054,
    id_11055,
    id_11056,
    id_11057,
    id_11058,
    id_11059,
    id_11060,
    id_11061
};

static_assert(scalar_function_id::id_11000 == 11'000);
static_assert(scalar_function_id::id_11061 == 11'061);

}  // namespace jogasaki::executor::function
