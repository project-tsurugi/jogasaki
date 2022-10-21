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

#include <thread>

#include <jogasaki/logging.h>
#include <jogasaki/kvs/error.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/utils/backoff_waiter.h>

namespace jogasaki::kvs {

sharksfin::TransactionOptions::TransactionType type(kvs::transaction_option::transaction_type type) {
    using k = kvs::transaction_option::transaction_type;
    using s = sharksfin::TransactionOptions::TransactionType;
    switch (type) {
        case k::occ: return s::SHORT;
        case k::ltx: return s::LONG;
        case k::read_only: return s::READ_ONLY;
    }
    fail();
}

transaction::transaction(
    kvs::database &db,
    kvs::transaction_option const& options
) : database_(std::addressof(db)) {
    sharksfin::TransactionOptions::WritePreserves wps{};
    std::vector<std::unique_ptr<kvs::storage>> stgs{}; // to keep storages during transaction_begin call
    stgs.reserve(options.write_preserves().size());
    for(auto&& wp : options.write_preserves()) {
        auto s = db.get_storage(wp);
        if(! s) {
            VLOG(log_error) << "Specified write preserved storage '" << wp << "' is not found.";
            fail(); //TODO
        }
        wps.emplace_back(s->handle());
        stgs.emplace_back(std::move(s));
    }
    sharksfin::TransactionOptions opts{
        type(options.type()),
        std::move(wps),
    };
    if(auto res = sharksfin::transaction_begin(db.handle(), opts, &tx_); res != sharksfin::StatusCode::OK) {
        fail();
    }
    if(options.type() == transaction_option::transaction_type::ltx || options.type() == transaction_option::transaction_type::read_only) {
        utils::backoff_waiter waiter{};
        while(true) {
            auto st = check_state().state_kind();
            VLOG(log_debug) << "checking for waiting transaction state:" << st;
            if (st != ::sharksfin::TransactionState::StateKind::WAITING_START) {
                break;
            }
            waiter();
        }
    }
}

transaction::~transaction() noexcept {
    if (active_) {
        sharksfin::transaction_abort(tx_, false);
    }
    sharksfin::transaction_dispose(tx_);
}

status transaction::commit(bool async) {
    // TODO remove retry here when scheduler is ready to handle waiting tasks
    auto rc = sharksfin::transaction_commit(tx_, async);
    if (rc == sharksfin::StatusCode::ERR_WAITING_FOR_OTHER_TRANSACTION) {
        VLOG(log_debug) << "commit() returns ERR_WAITING_FOR_OTHER_TX - waiting for others to finish";
        utils::backoff_waiter waiter{};
        while(true) {
            auto st = check_state().state_kind();
            VLOG(log_debug) << "checking for waiting transaction state:" << st;
            if (st != ::sharksfin::TransactionState::StateKind::WAITING_CC_COMMIT) {
                rc = sharksfin::transaction_commit(tx_, async);
                if (rc == sharksfin::StatusCode::ERR_WAITING_FOR_OTHER_TRANSACTION) {
                    fail();
                }
                break;
            }
            waiter();
        }
    }
    if(rc == sharksfin::StatusCode::OK ||
        rc == sharksfin::StatusCode::ERR_ABORTED ||
        rc == sharksfin::StatusCode::ERR_ABORTED_RETRYABLE) {
        active_ = false;
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

kvs::database* transaction::database() const noexcept {
    return database_;
}

status transaction::wait_for_commit(std::size_t timeout_ns) {
    auto rc = sharksfin::transaction_wait_commit(tx_, timeout_ns);
    return resolve(rc);
}

sharksfin::TransactionState transaction::check_state() noexcept {
    sharksfin::TransactionState result{};
    auto rc = sharksfin::transaction_check(tx_, result);
    if(rc != sharksfin::StatusCode::OK) {
        fail();
    }
    return result;
}

}

