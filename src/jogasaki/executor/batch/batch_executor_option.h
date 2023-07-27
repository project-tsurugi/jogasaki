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

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/executor/file/parquet_reader.h>

namespace jogasaki::executor::batch {

/**
 * @brief option object for batch executors
 */
class batch_executor_option {
public:
    static constexpr std::size_t undefined = static_cast<std::size_t>(-1);

    /**
     * @brief create empty object
     */
    batch_executor_option() = default;

    ~batch_executor_option() = default;
    batch_executor_option(batch_executor_option const& other) = default;
    batch_executor_option& operator=(batch_executor_option const& other) = default;
    batch_executor_option(batch_executor_option&& other) noexcept = default;
    batch_executor_option& operator=(batch_executor_option&& other) noexcept = default;

    /**
     * @brief create new object
     * @param max_concurrent_files the max number of files opened and processed by one batch_executor at a time
     * @param max_concurrent_blocks_per_file the max number of blocks processed by one batch_file_executor at a time
     */
    batch_executor_option(
        std::size_t max_concurrent_files,
        std::size_t max_concurrent_blocks_per_file
    ) noexcept;

    /**
     * @brief accessor for the max concurrent files value
     */
    [[nodiscard]] std::size_t max_concurrent_files() const noexcept;

    /**
     * @brief accessor for the max concurrent blocks value
     */
    [[nodiscard]] std::size_t max_concurrent_blocks_per_file() const noexcept;

private:
    std::size_t max_concurrent_files_{undefined};
    std::size_t max_concurrent_blocks_per_file_{undefined};
};

}