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
 * @brief the transaction state.
 */
class transaction_state {

    /**
     * @brief the transaction state kind enum.
     */
    enum class state_kind : std::int64_t {
        /**
         * @brief empty or unknown state
         */
        unknown = 0,

        /**
         * @brief transaction is not yet permitted to start
         * @details caller needs to wait or come back later in order to issue transactional operation
         */
        waiting_start,

        /**
         * @brief transaction started and is on-going
         * @details caller is permitted to issue transactional operation
         */
        started,

        /**
         * @brief commit of the transaction needs to wait
         * @details the commit request is submitted but not yet committed
         */
        waiting_cc_commit,

        /**
         * @brief transaction has been aborted
         * @details the transaction is aborted
         */
        aborted,

        /**
         * @brief transaction is not yet durable and waiting for it
         */
        waiting_durable,

        /**
         * @brief transaction became durable
         */
        durable
    };

    /**
     * @brief create default object (unknown state)
     */
    transaction_state() = default;

    /**
     * @brief create new object
     */
    explicit transaction_state(state_kind kind) noexcept;

    /**
     * @brief destruct the object
     */
    ~transaction_state() = default;

    /**
     * @brief copy constructor
     */
    transaction_state(transaction_state const& other) = default;

    /**
     * @brief copy assignment
     */
    transaction_state& operator=(transaction_state const& other) = default;

    /**
     * @brief move constructor
     */
    transaction_state(transaction_state&& other) noexcept = default;

    /**
     * @brief move assignment
     */
    transaction_state& operator=(transaction_state&& other) noexcept = default;

    /**
     * @brief returns the transaction operation kind.
     * @return the transaction operation kind
     */
    constexpr state_kind kind() const noexcept {
        return kind_;
    }

private:
    state_kind kind_{state_kind::unknown};
    // other information may be added here
};
}
