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
#include "batch_executor.h"

#include "batch_file_executor.h"
#include "batch_block_executor.h"

namespace jogasaki::executor::batch {

batch_executor::batch_executor(
    std::vector<std::string> files,
    batch_execution_info info
) noexcept:
    files_(std::move(files)),
    info_(std::move(info))
{}

std::pair<bool, bool> batch_executor::create_block(std::shared_ptr<batch_file_executor> const& file) {
    bool block_created = false;
    auto mb = info_.options().max_concurrent_blocks_per_file();
    std::size_t n = (mb == batch_executor_option::undefined) ? std::numeric_limits<std::size_t>::max() : mb;
    for(std::size_t i=0; i < n; ++i) {
        auto&& [success, blk] = file->next_block();
        if(! success) {
            return {false, false};
        }
        if(! blk) {
            break;
        }
        block_created = true;
    }
    return {true, block_created};
}

std::pair<bool, std::shared_ptr<batch_file_executor>> batch_executor::next_file() {
    while(true) { // to repeat on other file
        auto [success, file] = create_next_file();
        if (! success || ! file) {
            return {success, std::move(file)};
        }

        auto [s, block_created] = create_block(file);
        if (! s) {
            release(file.get());
            return {false, {}};
        }
        if (! block_created) {
            release(file.get());
            continue;
        }
        return {true, std::move(file)};
    }
}

std::pair<bool, std::shared_ptr<batch_file_executor>> batch_executor::create_next_file() {
    if(files_.empty()) {
        return {true, nullptr};
    }
    std::size_t cur{};
    do {
        cur = next_file_index_.load();
        if(cur > files_.size()-1) {
            return {true, nullptr};
        }
    } while (! next_file_index_.compare_exchange_strong(cur, cur+1));

    auto file = batch_file_executor::create_file_executor(
        files_[cur],
        info_,
        state_,
        this
    );
    if(! file) {
        return {false, nullptr};
    }

    {
        decltype(children_)::accessor acc{};
        if (children_.insert(acc, file.get())) {
            acc->second = std::move(file);
        }
        return {true, acc->second};
    }
}

batch_executor_option const &batch_executor::options() const noexcept {
    return info_.options();
}


bool batch_executor::bootstrap() {
    auto mf = info_.options().max_concurrent_files();
    std::size_t n = (mf == batch_executor_option::undefined) ? std::numeric_limits<std::size_t>::max() : mf;
    for(std::size_t i=0; i < n; ++i) {
        auto&& [success, f] = next_file();
        if(! success) {
            return false;
        }
        if(! f) {
            return true;
        }
    }
    return true;
}

std::shared_ptr<batch_file_executor> batch_executor::release(batch_file_executor *arg) {
    std::shared_ptr<batch_file_executor> ret{};
    decltype(children_)::accessor acc{};
    if (children_.find(acc, arg)) {
        ret = std::move(acc->second);
        children_.erase(acc);
    }
    if(info_.options().release_file_cb()) {
        info_.options().release_file_cb()(arg);
    }
    return ret;
}

std::size_t batch_executor::child_count() const noexcept {
    return children_.size();
}

std::shared_ptr<batch_execution_state> const &batch_executor::state() const noexcept {
    return state_;
}

std::shared_ptr<batch_executor>
batch_executor::create_batch_executor(
    std::vector<std::string> files,
    batch_execution_info info
) {
    return std::shared_ptr<batch_executor>(
        new batch_executor{
            std::move(files),
            std::move(info),
        }
    );
}

std::shared_ptr<batch_executor> batch_executor::shared() noexcept {
    return shared_from_this();
}

void batch_executor::end_of_file(batch_file_executor *arg) {
    auto [s, file] = next_file();
    if (! s) {
        return;
    }

    auto f = release(arg);
    (void) f;

    if (file) {
        return;
    }

    // no more file
    if(child_count() != 0) {
        // other files are in progress, so leave finalizing batch to it
        return;
    }
    // end of batch
    finish(info_, *state_);
}

}

