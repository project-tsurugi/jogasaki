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

namespace jogasaki::executor::batch {

using takatori::util::maybe_shared_ptr;

class batch_block_executor;
class batch_executor;

/**
 * @brief loader to conduct reading files and executing statements
 */
class cache_align batch_file_executor {
public:

    /**
     * @brief create empty object
     */
    batch_file_executor() = default;

    ~batch_file_executor() = default;
    batch_file_executor(batch_file_executor const& other) = delete;
    batch_file_executor& operator=(batch_file_executor const& other) = delete;
    batch_file_executor(batch_file_executor&& other) noexcept = delete;
    batch_file_executor& operator=(batch_file_executor&& other) noexcept = delete;

    batch_file_executor(
        std::string file,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::impl::database* db,
        batch_executor* parent = nullptr
    ) noexcept;

    /**
     * @brief conduct part of the load requests
     * @return running there is more to do
     * @return ok if all load requests are done
     * @return error if any error occurs
     */
    std::pair<bool, std::shared_ptr<batch_block_executor>> next_block();

    std::shared_ptr<batch_block_executor> release(batch_block_executor* arg);

    [[nodiscard]] batch_executor* parent() const noexcept;

    /**
     * @brief accessor to the number of blocks held by the target file
     * @return the block count
     */
    [[nodiscard]] std::size_t block_count() const noexcept;

    static std::shared_ptr<batch_file_executor> create_file_executor(
        std::string file,
        api::statement_handle prepared,
        maybe_shared_ptr<api::parameter_set const> parameters,
        api::impl::database* db,
        batch_executor* parent = nullptr
    );

private:
    std::string file_{};
    api::statement_handle prepared_{};
    maybe_shared_ptr<api::parameter_set const> parameters_{};
    api::impl::database* db_{};
    batch_executor* parent_{};
    tbb::concurrent_hash_map<batch_block_executor*, std::shared_ptr<batch_block_executor>> children_{};
    std::atomic_size_t next_block_index_{};
    std::size_t block_count_{};

    bool init();
};

}

