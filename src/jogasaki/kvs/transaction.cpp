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

#include <jogasaki/kvs/error.h>

namespace jogasaki::kvs {

transaction::transaction(class database &db, bool readonly) : database_(std::addressof(db)) {
    sharksfin::TransactionOptions options{};
    if (readonly) {
        options.operation_kind(sharksfin::TransactionOptions::OperationKind::READ_ONLY);
    }
    if(auto res = sharksfin::transaction_begin(db.handle(), options, &tx_); res != sharksfin::StatusCode::OK) {
        fail();
    }
}

transaction::~transaction() noexcept {
    if (active_) {
        sharksfin::transaction_abort(tx_, false);
    }
    sharksfin::transaction_dispose(tx_);
}

status transaction::commit(bool async) {
    auto rc = sharksfin::transaction_commit(tx_, async);
    if(rc == sharksfin::StatusCode::OK) {
        active_ = false;
        return status::ok;
    }
    return resolve(rc);
}

status transaction::abort() {
    auto rc = sharksfin::transaction_abort(tx_);
    if(rc == sharksfin::StatusCode::OK) {
        active_ = false;
        return status::ok;
    }
    return resolve(rc);
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

class database* transaction::database() const noexcept {
    return database_;
}

std::mutex& transaction::mutex() noexcept {
    return mutex_;
}

status transaction::wait_for_commit(std::size_t timeout_ns) {
    auto rc = sharksfin::transaction_wait_commit(tx_, timeout_ns);
    return resolve(rc);
}

}

