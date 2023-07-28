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
    api::statement_handle prepared,
    maybe_shared_ptr<const api::parameter_set> parameters,
    api::impl::database *db,
    callback_type cb,
    batch_executor_option opt
) noexcept:
    files_(std::move(files)),
    prepared_(prepared),
    parameters_(std::move(parameters)),
    db_(db),
    callback_(std::move(cb)),
    options_(opt)
{}

std::pair<bool, std::shared_ptr<batch_file_executor>> batch_executor::next_file() {
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
        prepared_,
        parameters_,
        db_,
        state_,
        this,
        options_.release_block_cb()
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

void batch_executor::finish() {
    if(finished_) return;
    if(callback_) {
        callback_();
    }
    finished_ = true;
}

batch_executor_option const &batch_executor::options() const noexcept {
    return options_;
}

void process_file(batch_file_executor& f, std::size_t mb) {
    for(std::size_t i=0; mb == batch_executor_option::undefined || i < mb; ++i) {
        auto&& [success, blk] = f.next_block();
        if(! success) {
            return;
        }
        if(! blk) {
            break;
        }
        blk->execute_statement();
    }
}

void batch_executor::bootstrap() {
    auto mf = options_.max_concurrent_files();
    for(std::size_t i=0; mf == batch_executor_option::undefined || i < mf; ++i) {
        auto&& [success, f] = next_file();
        if(! success) {
            break;
        }
        if(! f) {
            break;
        }
        process_file(*f, options_.max_concurrent_blocks_per_file());
    }
    if(state_->running_statements() == 0 && state_->error_aborting()) {
        finish();
    }
}

std::shared_ptr<batch_file_executor> batch_executor::release(batch_file_executor *arg) {
    std::shared_ptr<batch_file_executor> ret{};
    decltype(children_)::accessor acc{};
    if (children_.find(acc, arg)) {
        ret = std::move(acc->second);
        children_.erase(acc);
    }
    if(options_.release_file_cb()) {
        options_.release_file_cb()(arg);
    }
    return ret;
}

std::size_t batch_executor::child_count() const noexcept {
    return children_.size();
}

std::shared_ptr<batch_execution_state> const &batch_executor::state() const noexcept {
    return state_;
}

}

