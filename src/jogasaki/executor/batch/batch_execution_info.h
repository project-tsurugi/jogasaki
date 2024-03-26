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
#include <functional>
#include <string>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/statement_handle.h>
#include "jogasaki/api/parameter_set.h"

#include "batch_executor_option.h"

namespace jogasaki::executor::batch {

using takatori::util::maybe_shared_ptr;

/**
 * @brief static information on batch execution
 */
class batch_execution_info {
public:
    /**
     * @brief completion_callback type
     */
    using completion_callback_type = std::function<void(void)>;

    /**
     * @brief create new object
     */
    batch_execution_info() = default;

    ~batch_execution_info() = default;
    batch_execution_info(batch_execution_info const& other) = default;
    batch_execution_info& operator=(batch_execution_info const& other) = default;
    batch_execution_info(batch_execution_info&& other) noexcept = default;
    batch_execution_info& operator=(batch_execution_info&& other) noexcept = default;

    /**
     * @brief construct new object
     * @param prepared the statement to be executed
     * @param parameters the parameter prototype (types and names) whose value will be filled on execution
     * @param db the database instance
     * @param cb the callback to be called on batch execution completion
     * @param opt options to customize executor behavior
     */
    batch_execution_info(
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::impl::database* db,
        completion_callback_type cb = {},
        batch_executor_option opt = {}
    ) noexcept;

    /**
     * @brief accessor to the prepared statement for batch execution
     */
    [[nodiscard]] api::statement_handle prepared() const noexcept;

    /**
     * @brief accessor to the parameter prototype
     */
    [[nodiscard]] maybe_shared_ptr<api::parameter_set const> const& parameters() const noexcept;

    /**
     * @brief accessor to the database
     */
    [[nodiscard]] api::impl::database* db() const noexcept;

    /**
     * @brief accessor to the callback on batch execution completion
     */
    [[nodiscard]] completion_callback_type completion_callback() const noexcept;

    /**
     * @brief accessor to the batch execution options
     */
    [[nodiscard]] batch_executor_option const& options() const noexcept;

private:
    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};
    api::impl::database* db_{};
    completion_callback_type completion_callback_{};
    batch_executor_option options_{};
};


}
