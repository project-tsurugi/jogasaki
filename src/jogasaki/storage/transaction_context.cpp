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
#include "transaction_context.h"
#include "storage_context.h"

namespace jogasaki::storage {

transaction_context::transaction_context(storage_context &stg) : parent_(std::addressof(stg)) {
    sharksfin::TransactionOptions txopts{};
    if(auto res = sharksfin::transaction_begin(stg.handle(), txopts, &tx_); res != sharksfin::StatusCode::OK) {
        fail();
    }
}

transaction_context::~transaction_context() noexcept {
    if (active_) {
        sharksfin::transaction_abort(tx_, false);
    }
    sharksfin::transaction_dispose(tx_);
}

bool transaction_context::commit() {
    sharksfin::transaction_commit(tx_);
    active_ = false;
    return true;
}

bool transaction_context::abort() {
    sharksfin::transaction_abort(tx_);
    active_ = false;
    return true;
}

sharksfin::TransactionControlHandle transaction_context::control_handle() const noexcept {
    return tx_;
}

sharksfin::TransactionHandle transaction_context::handle() noexcept {
    if (!handle_) {
        if(auto res = sharksfin::transaction_borrow_handle(tx_, &handle_); res != sharksfin::StatusCode::OK) {
            fail();
        }
    }
    return handle_;
}

void transaction_context::open_scan() {
    if(sharksfin::StatusCode res = sharksfin::content_scan(handle(), parent_->default_storage(), {}, sharksfin::EndPointKind::UNBOUND,
                {}, sharksfin::EndPointKind::UNBOUND, &iterator_); res != sharksfin::StatusCode::OK) {
        fail();
    }
}

bool transaction_context::next_scan() {
    sharksfin::StatusCode res = sharksfin::iterator_next(iterator_);
    if (res == sharksfin::StatusCode::OK) {
        sharksfin::Slice key{};
        sharksfin::Slice value{};
        if(sharksfin::StatusCode res2 = sharksfin::iterator_get_key(iterator_, &key);res2 != sharksfin::StatusCode::OK) {
            fail();
        }
        if(sharksfin::StatusCode res2 = sharksfin::iterator_get_value(iterator_, &value);res2 != sharksfin::StatusCode::OK) {
            fail();
        }
        // TODO fill result
        return true;
    }
    if (res == sharksfin::StatusCode::NOT_FOUND) {
        return false;
    }
    fail();
}

void transaction_context::close_scan() {
    if(sharksfin::StatusCode res = sharksfin::iterator_dispose(iterator_); res != sharksfin::StatusCode::OK) {
        fail();
    }
}
}

