/*
 * Copyright 2018-2023 tsurugi project.
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
 * @brief the transaction priority.
 */
enum class transaction_priority : std::uint32_t {
    /**
     * @brief use default transaction priority.
     */
    priority_unspecified = 0U,

    /**
     * @brief halts the running transactions immediately.
     */
    interrupt,

    /**
     * @brief prevents new transactions and waits for the running transactions will end.
     */
    wait,

    /**
     * @brief halts the running transactions immediately, and keep lock-out until its end.
     */
    interrupt_exclude,

    /**
     * @brief prevents new transactions and waits for the running transactions will end, and keep lock-out until its end.
     */
    wait_exclude,
};

}
