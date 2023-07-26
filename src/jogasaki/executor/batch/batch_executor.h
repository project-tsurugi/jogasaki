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

#include <atomic>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/executor/file/parquet_reader.h>
#include "batch_executor_option.h"

namespace jogasaki::executor::batch {

using takatori::util::maybe_shared_ptr;

class batch_file_executor;
class batch_block_executor;

/**
 * @brief loader to conduct reading files and executing statements
 */
class cache_align batch_executor {
public:
    using callback_type = std::function<void(void)>;

    /**
     * @brief create empty object
     */
    batch_executor() = default;

    ~batch_executor() = default;
    batch_executor(batch_executor const& other) = delete;
    batch_executor& operator=(batch_executor const& other) = delete;
    batch_executor(batch_executor&& other) noexcept = delete;
    batch_executor& operator=(batch_executor&& other) noexcept = delete;

    /**
     * @brief create new object
     */
    batch_executor(
        std::vector<std::string> files,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::impl::database* db,
        callback_type cb,
        batch_executor_option opt = {}
    ) noexcept;


    /**
     * @brief conduct part of the load requests
     * @return running there is more to do
     * @return ok if all load requests are done
     * @return error if any error occurs
     */
    std::pair<bool, std::shared_ptr<batch_file_executor>> next_file();

    /**
     * @brief accessor to the total number of loaded records
     * @return the total number
     */
    [[nodiscard]] std::atomic_size_t& running_statements() noexcept;

    /**
     * @brief accessor to the error information
     * @return the error status and message
     */
    [[nodiscard]] std::pair<status, std::string> error_info() const noexcept;

    [[nodiscard]] bool error_info(status val, std::string_view msg) noexcept;

    std::atomic_bool& error_aborting() noexcept;

    [[nodiscard]] batch_executor_option const& options() const noexcept;

    void bootstrap();

    void finish();

private:
    std::vector<std::string> files_{};
    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};
    api::impl::database* db_{};
    callback_type callback_{};
    batch_executor_option options_{};

    std::atomic_size_t running_statements_{0};
    std::unordered_map<std::string, file::parameter> mapping_{};
    std::atomic_bool error_aborting_{false};
    std::atomic_bool finished_{false};
    std::atomic_size_t next_file_index_{};
    tbb::concurrent_hash_map<batch_file_executor*, std::shared_ptr<batch_file_executor>> children_{};

    std::atomic<status> status_code_{status::ok};
    std::string status_message_{};

};

}