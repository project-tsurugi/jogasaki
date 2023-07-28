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
#include <tbb/concurrent_hash_map.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/executor/file/parquet_reader.h>
#include <jogasaki/executor/batch/batch_execution_state.h>

namespace jogasaki::executor::batch {

using takatori::util::maybe_shared_ptr;

class batch_block_executor;
class batch_executor;

/**
 * @brief batch file executor
 * @details part of the block executor object tree and handles one of the bulk files for batch execution
 */
class cache_align batch_file_executor {
public:
    using release_callback_type = std::function<void(batch_block_executor*)>;

    /**
     * @brief create empty object
     */
    batch_file_executor() = default;

    ~batch_file_executor() = default;
    batch_file_executor(batch_file_executor const& other) = delete;
    batch_file_executor& operator=(batch_file_executor const& other) = delete;
    batch_file_executor(batch_file_executor&& other) noexcept = delete;
    batch_file_executor& operator=(batch_file_executor&& other) noexcept = delete;

    /**
     * @brief create new object
     * @param file the file path containing parameter
     * @param prepared the statement to be executed
     * @param parameters the parameter prototype (types and names) whose value will be filled on execution
     * @param db the database instance
     * @param parent the parent of this node. Can be nullptr for testing.
     */
    batch_file_executor(
        std::string file,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::impl::database* db,
        std::shared_ptr<batch_execution_state> state,
        batch_executor* parent = nullptr,
        release_callback_type release_cb = {}
    ) noexcept;

    /**
     * @brief create new block executor
     * @details create new block executor and own as a child. When the child is not necessary, it should be released
     * by `release()` function to save memory consumption. Otherwise the child is kept as long as this object is alive.
     * @return pair of Successful flag and the newly created block executor. If error occurs, Successful flag is false.
     * If there is no more file to process, Successful=true and nullptr is returned.
     */
    std::pair<bool, std::shared_ptr<batch_block_executor>> next_block();

    /**
     * @brief detach the child block executor from this object to release
     * @details detach the child block executor and returns its ownership
     * @param arg the block executor to be released
     * @return the block executor released
     * @return nullptr if the block executor is not owned by this object
     */
    std::shared_ptr<batch_block_executor> release(batch_block_executor* arg);

    /**
     * @brief accessor to the parent
     */
    [[nodiscard]] batch_executor* parent() const noexcept;

    /**
     * @brief accessor to the number of blocks held by the target file
     * @return the block count
     */
    [[nodiscard]] std::size_t block_count() const noexcept;

    /**
     * @brief accessor to the number of child nodes held by this object
     * @return the block count
     */
    [[nodiscard]] std::size_t child_count() const noexcept;

    /**
     * @brief factory function to construct file executor
     * @see constructor for the details of parameters
     * @return newly created file executor
     * @return nullptr when creation failed
     */
    static std::shared_ptr<batch_file_executor> create_file_executor(
        std::string file,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::impl::database* db,
        std::shared_ptr<batch_execution_state> state,
        batch_executor* parent = nullptr,
        release_callback_type release_cb = {}
    );

private:
    std::string file_{};
    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};
    api::impl::database* db_{};
    std::shared_ptr<batch_execution_state> state_{};
    batch_executor* parent_{};
    tbb::concurrent_hash_map<batch_block_executor*, std::shared_ptr<batch_block_executor>> children_{};
    std::atomic_size_t next_block_index_{};
    std::size_t block_count_{};
    release_callback_type release_cb_{};

    bool init();
};

}

