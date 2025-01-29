/*
 * Copyright 2018-2023 Project Tsurugi.
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

namespace jogasaki::executor::function::incremental {

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
enum function_id : std::size_t {
    id_10000 = yugawara::aggregate::declaration::minimum_builtin_function_id,
    id_10001,
    id_10002,
    id_10003,
    id_10004,
    id_10005,
    id_10006,
    id_10007,
    id_10008,
    id_10009,
    id_10010,
    id_10011,
    id_10012,
    id_10013,
    id_10014,
    id_10015,
    id_10016,
    id_10017,
    id_10018,
    id_10019,
    id_10020,
    id_10021,
    id_10022,
    id_10023,
    id_10024,
    id_10025,
    id_10026,
    id_10027,
    id_10028,
    id_10029,
    id_10030,
    id_10031,
    id_10032,
    id_10033,
    id_10034,
    id_10035,
    id_10036,
    id_10037,
    id_10038,
    id_10039,
    id_10040,
    id_10041,
    id_10042,
    id_10043,
    id_10044,
    id_10045,
    id_10046,
    id_10047,
    id_10048,
    id_10049,
};

static_assert(function_id::id_10000 == 10'000);
static_assert(function_id::id_10049 == 10'049);

}  // namespace jogasaki::executor::function::incremental
