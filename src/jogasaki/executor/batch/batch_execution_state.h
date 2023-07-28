/*
 * Copyright 2018-2020 tsurugi project.
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
#include <atomic>
#include <string>

#include <jogasaki/status.h>

namespace jogasaki::executor::batch {

/**
 * @brief option object for batch executors
 */
class batch_execution_state {
public:
    /**
     * @brief create new object
     */
    batch_execution_state() = default;

    ~batch_execution_state() = default;
    batch_execution_state(batch_execution_state const& other) = default;
    batch_execution_state& operator=(batch_execution_state const& other) = default;
    batch_execution_state(batch_execution_state&& other) noexcept = default;
    batch_execution_state& operator=(batch_execution_state&& other) noexcept = default;

    /**
     * @brief accessor to the error aborting flag
     * @details this is set when error status is set via error_info() and used to check current batch execution is going
     * to stop. This is useful to check the execution error state periodically, in order to proceed.
     * @note When error occurs during batch execution, the thread should callback and exit execution immediately.
     * Releasing executors are not done in small pieces, but it's left to the destruction of batch_executor to release
     * all in bulk.
     */
    bool error_aborting() const noexcept;

    /**
     * @brief accessor to the error information
     * @return the error status and message
     */
    [[nodiscard]] std::pair<status, std::string> error_info() const noexcept;

    /**
     * @brief setter for the error information
     * @returns true if the passed status is set
     * @returns false if the status is already set to a value other than status::ok, and setting to new value failed
     * @note this function is thread safe
     */
    bool error_info(status val, std::string_view msg) noexcept;

    /**
     * @brief accessor to the number of statements being scheduled/executed
     * @return the running statement count
     */
    [[nodiscard]] std::atomic_size_t& running_statements() noexcept;

private:
    std::atomic<status> status_code_{status::ok};
    std::string status_message_{};
    std::atomic_bool error_aborting_{false};
    std::atomic_size_t running_statements_{0};
};

}