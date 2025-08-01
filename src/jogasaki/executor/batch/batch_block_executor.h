/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <string>
#include <unordered_map>
#include <utility>

#include <takatori/util/maybe_shared_ptr.h>
#include <tateyama/utils/cache_align.h>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/executor/batch/batch_execution_info.h>
#include <jogasaki/executor/batch/batch_execution_state.h>
#include <jogasaki/executor/batch/batch_executor_option.h>
#include <jogasaki/executor/file/file_reader.h>
#include <jogasaki/executor/file/loader.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::executor::batch {

using takatori::util::maybe_shared_ptr;

class batch_executor;
class batch_file_executor;

/**
 * @brief batch block executor
 * @details part of the block executor object tree and handles a block in a file
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
     * @brief execute statement
     * @details execute next statement in this block. New line is read from the block and statement is scheduled.
     * @returns pair of `Success`, `Found` flags. `Success` is false if reading new line is not successful or
     * there is an error during processing the new line. `Found` is meaningful only when `Success` is true.
     * `Found` is false if there is no new line to read. `Found` is true if reading is successful and statement task
     * is scheduled correctly.
     */
    std::pair<bool, bool> next_statement();

    /**
     * @brief callback function on ending statement
     */
    void end_of_statement();

    /**
     * @brief accessor to the total number of statements executed
     * @return the statement count
     */
    [[nodiscard]] std::size_t statements_executed() const noexcept;

    /**
     * @brief accessor to the parent
     */
    [[nodiscard]] batch_file_executor* parent() const noexcept;

    /**
     * @brief accessor to the top level batch executor
     */
    [[nodiscard]] batch_executor* root() const noexcept;

    /**
     * @brief accessor to the execution state
     * @return the bath executor state
     */
    [[nodiscard]] std::shared_ptr<batch_execution_state> const& state() const noexcept;
    /**
     * @brief factory function to construct block executor
     * @see constructor for the details of parameters
     * @return newly created block executor
     * @return nullptr when creation failed
     */
    static std::shared_ptr<batch_block_executor> create_block_executor(
        std::string file,
        std::size_t block_index,
        batch_execution_info info,
        std::shared_ptr<batch_execution_state> state,
        batch_file_executor* parent = nullptr
    );

private:
    std::string file_{};
    std::size_t block_index_{};
    std::shared_ptr<file::file_reader> reader_{};
    batch_execution_info info_{};
    std::shared_ptr<batch_execution_state> state_{};
    batch_file_executor* parent_{};

    std::shared_ptr<transaction_context> tx_{};
    std::atomic_size_t statements_executed_{0};
    maybe_shared_ptr<meta::external_record_meta> meta_{};
    std::unordered_map<std::string, file::parameter> mapping_{};

    /**
     * @brief create new object
     * @param file the file path containing parameter
     * @param block_index 0-origin index of the block to process that is read from the file
     * @param parent the parent of this node. Can be nullptr for testing.
     */
    batch_block_executor(
        std::string file,
        std::size_t block_index,
        batch_execution_info info,
        std::shared_ptr<batch_execution_state> state,
        batch_file_executor* parent = nullptr
    ) noexcept;

};

}
