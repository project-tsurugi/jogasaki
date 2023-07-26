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

#include <jogasaki/api/database.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/executor/file/parquet_reader.h>

namespace jogasaki::executor::batch {

using takatori::util::maybe_shared_ptr;

class batch_executor;
class batch_file_executor;

/**
 * @brief loader to conduct reading files and executing statements
 */
class cache_align batch_block_executor {
public:

    /**
     * @brief create empty object
     */
    batch_block_executor() = default;

    ~batch_block_executor() = default;
    batch_block_executor(batch_block_executor const& other) = delete;
    batch_block_executor& operator=(batch_block_executor const& other) = delete;
    batch_block_executor(batch_block_executor&& other) noexcept = delete;
    batch_block_executor& operator=(batch_block_executor&& other) noexcept = delete;

    /**
     * @brief create new object
     */
    batch_block_executor(
        std::string file,
        std::size_t block_index,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::impl::database* db,
        batch_file_executor* parent = nullptr
    ) noexcept;

    /**
     * @brief conduct part of the load requests
     * @return running there is more to do
     * @return ok if all load requests are done
     * @return error if any error occurs
     */
    void execute_statement();

    /**
     * @brief accessor to the total number of loaded records
     * @return the total number
     */
    [[nodiscard]] std::size_t statements_executed() const noexcept;

    /**
     * @brief accessor to the error information
     * @return the error status and message
     */
    [[nodiscard]] std::pair<status, std::string> error_info() const noexcept;

    [[nodiscard]] batch_file_executor* parent() const noexcept;

    [[nodiscard]] batch_executor* root() const noexcept;

    static std::shared_ptr<batch_block_executor> create_block_executor(
        std::string file,
        std::size_t block_index,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::impl::database* db,
        batch_file_executor* parent = nullptr
    );
private:
    std::string file_{};
    std::size_t block_index_{};
    std::shared_ptr<file::parquet_reader> reader_{};
    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};
    api::impl::database* db_{};
    batch_file_executor* parent_{};

    std::unique_ptr<api::impl::transaction> tx_{};
    std::atomic_size_t statements_executed_{0};
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    std::unordered_map<std::string, file::parameter> mapping_{};
    status status_{status::ok};
    std::string msg_{};

    void process_next();
};

}
