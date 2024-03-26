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
#include "batch_executor_option.h"

#include <utility>

namespace jogasaki::executor::batch {

batch_executor_option::batch_executor_option(
    std::size_t max_concurrent_files,
    std::size_t max_concurrent_blocks_per_file,
    release_file_callback_type release_file_cb,
    release_block_callback_type release_block_cb
) noexcept:
    max_concurrent_files_(max_concurrent_files),
    max_concurrent_blocks_per_file_(max_concurrent_blocks_per_file),
    release_file_cb_(std::move(release_file_cb)),
    release_block_cb_(std::move(release_block_cb))
{}

std::size_t batch_executor_option::max_concurrent_files() const noexcept {
    return max_concurrent_files_;
}

std::size_t batch_executor_option::max_concurrent_blocks_per_file() const noexcept {
    return max_concurrent_blocks_per_file_;
}

batch_executor_option::release_file_callback_type batch_executor_option::release_file_cb() const noexcept {
    return release_file_cb_;
}

batch_executor_option::release_block_callback_type batch_executor_option::release_block_cb() const noexcept {
    return release_block_cb_;
}

batch_executor_option::batch_executor_option(
    batch_executor_option::release_file_callback_type release_file_cb,
    batch_executor_option::release_block_callback_type release_block_cb
) noexcept:
    release_file_cb_(std::move(release_file_cb)),
    release_block_cb_(std::move(release_block_cb))
{}
}
