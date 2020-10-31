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
#include "transaction.h"
#include "database.h"

namespace jogasaki::kvs {

transaction::transaction(database &db) : parent_(std::addressof(db)) {
    sharksfin::TransactionOptions txopts{};
    if(auto res = sharksfin::transaction_begin(db.handle(), txopts, &tx_); res != sharksfin::StatusCode::OK) {
        fail();
    }
}

transaction::~transaction() noexcept {
    if (active_) {
        sharksfin::transaction_abort(tx_, false);
    }
    sharksfin::transaction_dispose(tx_);
}

bool transaction::commit() {
    sharksfin::transaction_commit(tx_);
    active_ = false;
    return true;
}

bool transaction::abort() {
    sharksfin::transaction_abort(tx_);
    active_ = false;
    return true;
}

sharksfin::TransactionControlHandle transaction::control_handle() const noexcept {
    return tx_;
}

sharksfin::TransactionHandle transaction::handle() noexcept {
    if (!handle_) {
        if(auto res = sharksfin::transaction_borrow_handle(tx_, &handle_); res != sharksfin::StatusCode::OK) {
            fail();
        }
    }
    return handle_;
}

}

