/*
 * Copyright 2018-2022 tsurugi project.
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
#include "transaction_context.h"

#include <jogasaki/kvs/database.h>

namespace jogasaki {

transaction_context::transaction_context(std::shared_ptr<kvs::transaction> transaction) :
    transaction_(std::move(transaction)),
    id_(id_source_++)
{}

transaction_context::operator kvs::transaction&() const noexcept {  //NOLINT
    return *transaction_;
}

std::shared_ptr<kvs::transaction> const& transaction_context::object() const {
    return transaction_;
}

std::size_t transaction_context::id() const noexcept {
    return id_;
}

transaction_context::operator bool() const noexcept {
    return transaction_ != nullptr;
}

status transaction_context::commit(bool async) {
    return transaction_->commit(async);
}

status transaction_context::wait_for_commit(std::size_t timeout_ns) {
    return transaction_->wait_for_commit(timeout_ns);
}

status transaction_context::abort() {
    return transaction_->abort();
}

sharksfin::TransactionControlHandle transaction_context::control_handle() const noexcept {
    return transaction_->control_handle();
}

sharksfin::TransactionHandle transaction_context::handle() noexcept {
    return transaction_->handle();
}

kvs::database* transaction_context::database() const noexcept {
    return transaction_->database();
}

bool transaction_context::increment_worker_count(uint32_t& worker_index) {
    return mgr_.increment_and_set_on_zero(worker_index);
}

bool transaction_context::decrement_worker_count() {
    return mgr_.decrement_and_clear_on_zero();
}

std::shared_ptr<transaction_context> wrap(std::unique_ptr<kvs::transaction> arg) noexcept {
    return std::make_shared<transaction_context>(std::shared_ptr<kvs::transaction>{std::move(arg)});
}

bool details::worker_manager::increment_and_set_on_zero(uint32_t& worker_index) {
    std::size_t cur = use_count_and_worker_id_.load();
    std::size_t next{};
    std::uint32_t cnt = 0;
    std::uint32_t wid = 0;
    do {
        cnt = upper(cur);
        wid = lower(cur);
        if (cnt == 0) {
            wid = worker_index;
        }
        if (wid != worker_index) {
            worker_index = wid;
            return false;
        }
        next = cnt + 1;
        next <<= 32U;
        next |= wid;
    } while (! use_count_and_worker_id_.compare_exchange_strong(cur, next));
    return true;
}

bool details::worker_manager::decrement_and_clear_on_zero() {
    std::size_t cur = use_count_and_worker_id_.load();
    std::size_t next{};
    std::uint32_t cnt = 0;
    std::uint32_t wid = 0;
    do {
        cnt = upper(cur);
        if (cnt == 0) {
            // just ignore
            return true;
        }
        wid = lower(cur);
        if (cnt == 1) {
            wid = empty_worker;
        }
        next = cnt - 1;
        next <<= 32U;
        next |= wid;
    } while (! use_count_and_worker_id_.compare_exchange_strong(cur, next));
    return cnt == 1;
}

std::uint32_t details::worker_manager::worker_id() const noexcept {
    std::size_t cur = use_count_and_worker_id_.load();
    return lower(cur);
}

std::uint32_t details::worker_manager::use_count() const noexcept {
    std::size_t cur = use_count_and_worker_id_.load();
    return upper(cur);
}
}

