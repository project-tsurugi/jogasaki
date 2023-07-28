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
#include "batch_file_executor.h"

#include "batch_executor.h"
#include "batch_block_executor.h"

namespace jogasaki::executor::batch {

std::pair<bool, std::shared_ptr<batch_block_executor>> batch_file_executor::next_block() {
    std::size_t cur{};
    do {
        cur = next_block_index_.load();
        if(cur > block_count_-1) {
            return {true, nullptr};
        }
    } while (! next_block_index_.compare_exchange_strong(cur, cur+1));

    auto blk = batch_block_executor::create_block_executor(
        file_,
        cur,
        info_,
        state_,
        this
    );

    {
        decltype(children_)::accessor acc{};
        if (children_.insert(acc, blk.get())) {
            acc->second = std::move(blk);
        }
        return {true, acc->second};
    }
}

std::shared_ptr<batch_block_executor> batch_file_executor::release(batch_block_executor *arg) {
    std::shared_ptr<batch_block_executor> ret{};
    decltype(children_)::accessor acc{};
    if (children_.find(acc, arg)) {
        ret = std::move(acc->second);
        children_.erase(acc);
    }
    if(info_.options().release_block_cb()) {
        info_.options().release_block_cb()(arg);
    }
    return ret;
}

batch_executor *batch_file_executor::parent() const noexcept {
    return parent_;
}

batch_file_executor::batch_file_executor(
    std::string file,
    batch_execution_info info,
    std::shared_ptr<batch_execution_state> state,
    batch_executor* parent
) noexcept:
    file_(std::move(file)),
    info_(std::move(info)),
    state_(std::move(state)),
    parent_(parent)
{}

std::size_t batch_file_executor::block_count() const noexcept {
    return block_count_;
}

bool batch_file_executor::init() {
    // create reader and check file metadata
    auto reader = file::parquet_reader::open(file_, nullptr, file::parquet_reader::index_unspecified);
    if(! reader) {
        (void) state_->error_info(status::err_io_error, "opening parquet file failed.");
        finish(info_, *state_);
        return false;
    }
    block_count_ = reader->row_group_count();
    return true;
}

std::shared_ptr<batch_file_executor>
batch_file_executor::create_file_executor(
    std::string file,
    batch_execution_info info,
    std::shared_ptr<batch_execution_state> state,
    batch_executor *parent
) {
    auto ret = std::make_shared<batch_file_executor>(
        std::move(file),
        std::move(info),
        std::move(state),
        parent
    );
    if(! ret->init()) {
        return {};
    }
    return ret;
}

std::size_t batch_file_executor::child_count() const noexcept {
    return children_.size();
}

std::shared_ptr<batch_execution_state> const &batch_file_executor::state() const noexcept {
    return state_;
}

}

