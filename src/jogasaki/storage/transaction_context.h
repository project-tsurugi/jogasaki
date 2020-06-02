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

#include <glog/logging.h>
#include <takatori/util/fail.h>
#include <sharksfin/api.h>
#include <sharksfin/Environment.h>

namespace jogasaki::storage {

/**
 * @brief context for the transaction
 */
class transaction_context {
public:
    /**
     * @brief create default context object
     */
    transaction_context(storage_context const& stg) {
        sharksfin::TransactionOptions txopts{};
        if(auto res = sharksfin::transaction_begin(stg.handle(), txopts, &tx_); res != sharksfin::StatusCode::OK) {
            takatori::util::fail();
        }
    }

    /**
     * @brief create default context object
     */
    ~transaction_context() noexcept {
        if (active_) {
            sharksfin::transaction_abort(tx_, false);
        }
        sharksfin::transaction_dispose(tx_);
    }

    // TODO other constructors

    bool commit() {
        sharksfin::transaction_commit(tx_);
        active_ = false;
        return true;
    }

    bool abort() {
        sharksfin::transaction_abort(tx_);
        active_ = false;
        return true;
    }

    sharksfin::TransactionControlHandle control_handle() const noexcept {
        return tx_;
    }

    sharksfin::TransactionHandle handle() noexcept {
        if (!handle_) {
            if(auto res = sharksfin::transaction_borrow_handle(tx_, &handle_); res != sharksfin::StatusCode::OK) {
                takatori::util::fail();
            }
        }
        return handle_;
    }
private:
    sharksfin::TransactionControlHandle tx_{};
    sharksfin::TransactionHandle handle_{};
    bool active_{true};
};

}

