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

#include <cstdint>

namespace jogasaki::api::kvsservice {

/**
 * @brief the transaction type.
 */
enum class transaction_type : std::uint32_t {
    /**
     * @brief use default transaction type.
     */
    unspecified = 0U,

    /**
     * @brief short transactions (optimistic concurrency control).
     */
    occ,

    /**
     * @brief long transactions (pessimistic concurrency control).
     */
    ltx,

    /**
     * @brief read only transactions (may be abort-free).
     */
    read_only,
};

}

