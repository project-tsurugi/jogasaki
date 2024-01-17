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

#include <atomic>
#include <memory>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/executor/file/file_reader.h>
#include <jogasaki/executor/file/loader.h>
#include "batch_executor_option.h"
#include "batch_execution_state.h"
#include "batch_execution_info.h"

namespace jogasaki::executor::batch {

using takatori::util::maybe_shared_ptr;

class batch_file_executor;
class batch_block_executor;

/**
 * @brief batch executor
 * @details the top level object of batch execution hierarchy tree (with file/block executors).
 * Except the root, tree nodes are dynamically constructed and destroyed as the batch execution proceeds.
 * Each of the object tree node is owned by its parent, and only the ownership of the root (batch_executor)
 * should be managed by user of this object.
 */
class cache_align batch_executor : public std::enable_shared_from_this<batch_executor> {
public:
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
     * @brief create new file executor
     * @details create new file executor and own as a child
     * @return pair of Successful flag and the newly created file executor. If error occurs, Successful flag is false.
     * If there is no more file to process, Successful=true and nullptr is returned.
     */
    std::pair<bool, std::shared_ptr<batch_file_executor>> next_file();

    /**
     * @brief detach the child file executor from this object to release
     * @details detach the child file executor and returns its ownership
     * @param arg the file executor to be released
     * @return the pair of file executor released (nullptr if the file executor is not owned by this object)
     * and the remaining incomplete files
     */
    std::pair<std::shared_ptr<batch_file_executor>, std::size_t> release(batch_file_executor *arg);

    /**
     * @brief accessor to the number of child nodes held by this object
     * @return the block count
     */
    [[nodiscard]] std::size_t child_count() const noexcept;

    /**
     * @brief accessor to the options
     * @return the bath executor options
     */
    [[nodiscard]] batch_executor_option const& options() const noexcept;

    /**
     * @brief request bootstrap
     * @details this function create child file/block executors and schedule statements execution. Useful to bulk
     * invoke children.
     * @returns true when successful
     * @returns false on error
     */
    bool bootstrap();

    /**
     * @brief accessor to the execution state
     * @return the bath executor state
     */
    [[nodiscard]] std::shared_ptr<batch_execution_state> const& state() const noexcept;

    /**
     * @brief get shared ptr for `this`
     * @return the shared_ptr of this object
     */
    [[nodiscard]] std::shared_ptr<batch_executor> shared();

    /**
     * @brief callback function on ending file
     */
    void end_of_file(batch_file_executor* arg);

    /**
     * @brief factory function to construct executor
     * @see constructor for the details of parameters
     * @return newly created executor
     * @return nullptr when creation failed
     */
    static std::shared_ptr<batch_executor> create_batch_executor(
        std::vector<std::string> files,
        batch_execution_info info
    );

private:
    std::vector<std::string> files_{};
    batch_execution_info info_{};
    std::shared_ptr<batch_execution_state> state_{std::make_shared<batch_execution_state>()};

    std::unordered_map<std::string, file::parameter> mapping_{};
    std::atomic_size_t next_file_index_{};
    tbb::concurrent_hash_map<batch_file_executor*, std::shared_ptr<batch_file_executor>> children_{};
    std::atomic_size_t remaining_file_count_{};

    /**
     * @brief create new object
     * @param files the files holding parameter values
     * @param info the static execution information
     */
    batch_executor(
        std::vector<std::string> files,
        batch_execution_info info
    ) noexcept;

    /**
     * @brief create new file executor
     * @details create new file executor and own as a child
     * @return pair of Successful flag and the newly created file executor. If error occurs, Successful flag is false.
     * If there is no more file to process, Successful=true and nullptr is returned.
     */
    std::pair<bool, std::shared_ptr<batch_file_executor>> create_next_file();

    std::pair<bool, bool> create_blocks(std::shared_ptr<batch_file_executor> const& file);
};

}
