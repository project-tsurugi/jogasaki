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

#include <atomic>
#include <cstddef>
#include <memory>

#include <jogasaki/transaction_state_kind.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki {

namespace details {

bool transition_allowed(transaction_state_kind cur, transaction_state_kind dest);

}  // namespace details

/**
 * @brief transaction state control object
 */
class cache_align transaction_state {
public:
    /**
     * @brief construct new object
     */
    constexpr transaction_state() = default;

    transaction_state(transaction_state const&) = delete;
    transaction_state(transaction_state&&) = delete;
    transaction_state& operator=(transaction_state const&) = delete;
    transaction_state& operator=(transaction_state&&) = delete;

    /**
     * @brief destruct object
     */
    ~transaction_state() = default;

    /**
     * @brief accessor to the current state kind
     * @return the current state kind
     */
    [[nodiscard]] transaction_state_kind kind() const noexcept;

    /**
     * @brief setter of the current state kind
     * @param desired the state kind to set
     * @details set the state kind to the given value after checking if the
     * transition from the current state is valid. If the transition is not valid,
     * this function does nothing.
     */
    void set(transaction_state_kind desired) noexcept;

    /**
     * @brief setter of the current state kind
     * @details set the state kind to the given value if the current state is same as `expected`
     * @return true if the state is changed
     * @return false otherwise
     */
    bool set_if(transaction_state_kind expected, transaction_state_kind desired) noexcept;

private:

    std::atomic<transaction_state_kind> state_{transaction_state_kind::undefined};

};

static_assert(std::is_trivially_destructible_v<transaction_state>);
static_assert(std::alignment_of_v<transaction_state> == 64);
static_assert(sizeof(transaction_state) == 64);

} // namespace jogasaki

