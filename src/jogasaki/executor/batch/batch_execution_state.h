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
#include <memory>

#include <jogasaki/status.h>
#include <jogasaki/error/error_info.h>

namespace jogasaki::executor::batch {

class batch_execution_info;

/**
 * @brief dynamic state information on the batch execution
 */
class batch_execution_state {
public:
    /**
     * @brief create new object
     */
    batch_execution_state() = default;

    ~batch_execution_state() = default;
    batch_execution_state(batch_execution_state const& other) = delete;
    batch_execution_state& operator=(batch_execution_state const& other) = delete;
    batch_execution_state(batch_execution_state&& other) noexcept = delete;
    batch_execution_state& operator=(batch_execution_state&& other) noexcept = delete;

    /**
     * @brief accessor to the error aborting flag
     * @details this is set when error status is set via error_info() and used to check current batch execution is going
     * to stop. This is useful to check the execution error state periodically, in order to proceed.
     * @note When error occurs during batch execution, the thread should completion_callback and exit execution immediately.
     * Releasing executors are not done in small pieces, but it's left to the destruction of batch_executor to release
     * all in bulk.
     */
    [[nodiscard]] bool error_aborting() const noexcept;

    /**
     * @brief accessor to the execution result status
     * @return the status
     */
    [[nodiscard]] status status_code() const noexcept;

    /**
     * @brief accessor to the error information
     * @return the error info
     */
    [[nodiscard]] std::shared_ptr<error::error_info> error_info() const noexcept;

    /**
     * @brief setter for the error information
     * @returns true if the passed status is set
     * @returns false if the non-empty error info is already set
     * @note this function is thread safe
     */
    bool set_error_status(status st, std::shared_ptr<error::error_info> info) noexcept;

    /**
     * @brief accessor to the number of statements being scheduled/executed
     * @return the running statement count
     */
    [[nodiscard]] std::atomic_size_t& running_statements() noexcept;

    /**
     * @brief setter for the finished flag
     * @returns true if the finished state successfully changed from false to true
     * @returns false otherwise
     * @note this function is thread safe
     */
    bool finish() noexcept;

    /**
     * @brief accessor to the number of statements being scheduled/executed
     * @return the running statement count
     */
    [[nodiscard]] bool finished() const noexcept;

private:
    std::atomic<status> status_code_{status::ok};
    std::atomic_bool error_aborting_{false};
    std::atomic_size_t running_statements_{0};
    std::atomic_bool finished_{false};
    std::shared_ptr<error::error_info> error_info_{};
};

/**
 * @brief finish the batch execution
 * @details declare end of batch execution and invoke completion callback.
 * This function is idempotent and calling more than once does no harm.
 * @param info static information on the batch execution
 * @param state dynamic state of the batch execution
 * @note this function is thread safe and multiple threads can call simultaneously
 */
void finish(batch_execution_info const& info, batch_execution_state& state);

}