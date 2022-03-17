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

namespace jogasaki::kvs {

transaction::transaction(
    kvs::database &db,
    bool readonly,
    bool is_long,
    std::vector<std::string> const& write_preserves
) : database_(std::addressof(db)) {
    sharksfin::TransactionOptions::WritePreserves wps{};
    std::vector<std::unique_ptr<kvs::storage>> stgs{}; // to keep storages during transaction_begin call
    stgs.reserve(write_preserves.size());
    for(auto&& wp : write_preserves) {
        auto s = db.get_storage(wp);
        if(! s) {
            VLOG(log_error) << "Specified write preserved storage '" << wp << "' is not found.";
            fail(); //TODO
        }
        wps.emplace_back(s->handle());
        stgs.emplace_back(std::move(s));
    }
    sharksfin::TransactionOptions options{
        is_long ? sharksfin::TransactionOptions::TransactionType::LONG : sharksfin::TransactionOptions::TransactionType::SHORT,
        std::move(wps)
    };
    if (readonly) {
        options.transaction_type(sharksfin::TransactionOptions::TransactionType::READ_ONLY);
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
    sharksfin::StatusCode rc{};
    // TODO remove retry here when scheduler is ready to handle waiting tasks
    std::size_t trial = 30;
    while(trial > 0 && (rc = sharksfin::transaction_commit(tx_, async)) == sharksfin::StatusCode::ERR_WAITING_FOR_OTHER_TX) {
        std::this_thread::sleep_for(std::chrono::milliseconds{100});
        --trial;
    }
    if (trial == 0) {
        VLOG(log_error) << "commit failed and giving up retries.";
        auto res = abort();
        VLOG(log_warning) << "abort returns:" << res;
        return status::err_aborted;
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

std::mutex& transaction::mutex() noexcept {
    return mutex_;
}

status transaction::wait_for_commit(std::size_t timeout_ns) {
    auto rc = sharksfin::transaction_wait_commit(tx_, timeout_ns);
    return resolve(rc);
}

}

