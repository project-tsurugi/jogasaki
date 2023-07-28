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
#include "batch_execution_state.h"

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
class cache_align batch_executor {
public:
    /**
     * @brief callback type
     */
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
     * @param files the files holding parameter values
     * @param prepared the statement to be executed
     * @param parameters the parameter prototype (types and names) whose value will be filled on execution
     * @param db the database instance
     * @param cb the callback to be called on batch execution completion
     * @param opt options to customize executor behavior
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
     * @return the file executor released
     * @return nullptr if the file executor is not owned by this object
     */
    std::shared_ptr<batch_file_executor> release(batch_file_executor* arg);

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
     */
    void bootstrap();

    /**
     * @brief declare finishing the batch execution
     * @details finish the batch execution and invoke completion callback
     */
    void finish();

    /**
     * @brief accessor to the execution state
     * @return the bath executor state
     */
    [[nodiscard]] std::shared_ptr<batch_execution_state> const& state() const noexcept;
private:
    std::vector<std::string> files_{};
    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};
    api::impl::database* db_{};
    callback_type callback_{};
    batch_executor_option options_{};
    std::shared_ptr<batch_execution_state> state_{std::make_shared<batch_execution_state>()};

    std::unordered_map<std::string, file::parameter> mapping_{};
    std::atomic_bool finished_{false};
    std::atomic_size_t next_file_index_{};
    tbb::concurrent_hash_map<batch_file_executor*, std::shared_ptr<batch_file_executor>> children_{};


};

}