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

using ::takatori::util::fail;

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
            fail();
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
                fail();
            }
        }
        return handle_;
    }

    void open_scan() {
        if(sharksfin::StatusCode res = sharksfin::content_scan(handle_, nullptr, {}, sharksfin::EndPointKind::EXCLUSIVE,
            {}, sharksfin::EndPointKind::EXCLUSIVE, &iterator_); res != sharksfin::StatusCode::OK) {
            fail();
        }
    }

    bool next_scan() {
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

    void close_scan() {
        if(sharksfin::StatusCode res = sharksfin::iterator_dispose(iterator_); res != sharksfin::StatusCode::OK) {
            fail();
        }
    }
private:
    sharksfin::TransactionControlHandle tx_{};
    sharksfin::TransactionHandle handle_{};
    sharksfin::IteratorHandle iterator_{};
    bool active_{true};
};

}

