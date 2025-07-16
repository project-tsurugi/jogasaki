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
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string_view>

#include <sharksfin/CallResult.h>
#include <sharksfin/api.h>

#include <jogasaki/commit_profile.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/transaction.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki {

/**
 * @brief termination state
 * @details task use count and going-to-{commit, abort} flags packed in unsigned
 * 64-bit integer
 */
class termination_state {

public:

    //@brief bit mask for going-to-abort flag
    static constexpr std::uint64_t bit_mask_going_to_abort = 1UL << 63U;

    //@brief bit mask for going-to-commit flag
    static constexpr std::uint64_t bit_mask_going_to_commit = 1UL << 62U;

    //@brief bit mask for task use count
    static constexpr std::uint64_t bit_mask_task_use_count = bit_mask_going_to_commit - 1;

    /**
     * @brief construct new object
     */
    constexpr termination_state() = default;

    termination_state(termination_state const&) = default;
    termination_state(termination_state&&) = default;
    termination_state& operator=(termination_state const&) = default;
    termination_state& operator=(termination_state&&) = default;

    /**
     * @brief destruct object
     */
    ~termination_state() = default;

    /**
     * @brief task use count accessor
     * @return the number of in-transaction tasks using the transaction context
     */
    [[nodiscard]] std::size_t task_use_count() const noexcept {
        return static_cast<std::size_t>(state_ & bit_mask_task_use_count);
    }

    /**
     * @brief task use count setter
     * @param value the new task use count
     */
    void task_use_count(std::size_t value) noexcept {
        state_ = (state_ & ~bit_mask_task_use_count) | (value & bit_mask_task_use_count);
    }

    /**
     * @brief accessor for the going-to-abort flag
     * @return true if the transaction is going to abort, false otherwise
     */
    [[nodiscard]] bool going_to_abort() const noexcept {
        return (state_ & bit_mask_going_to_abort) != 0;
    }

    /**
     * @brief set the going-to-abort flag
     */
    void set_going_to_abort() noexcept {
        state_ |= bit_mask_going_to_abort;
    }

    /**
     * @brief accessor for the going-to-commit flag
     * @return true if the transaction is going to commit, false otherwise
     */
    [[nodiscard]] bool going_to_commit() const noexcept {
        return (state_ & bit_mask_going_to_commit) != 0;
    }

    /**
     * @brief set the going-to-commit flag
     */
    void set_going_to_commit() noexcept {
        state_ |= bit_mask_going_to_commit;
    }

    /**
     * @brief getter for whole value for flags and counter
     */
    explicit operator std::uint64_t() const noexcept {
        return state_;
    }

    /**
     * @brief clear the state
     */
    void clear() noexcept {
        state_ = 0;
    }

    /**
     * @brief check if in-transaction task exists using the transaction context
     * @return true if no in-transaction task exists using the transaction
     * context, false otherwise
     */
    [[nodiscard]] bool task_empty() const noexcept {
        return task_use_count() == 0;
    }

private:
    std::uint64_t state_{};
};

static_assert(std::is_trivially_copyable_v<termination_state>);
static_assert(std::is_trivially_destructible_v<termination_state>);
static_assert(std::alignment_of_v<termination_state> == 8);
static_assert(sizeof(termination_state) == 8);

}

