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
#include <jogasaki/logging_helper.h>
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
    kvs::database &db
) :
    database_(std::addressof(db))
{}

transaction::~transaction() noexcept {
    if (active_) {
        sharksfin::transaction_abort(tx_, false);
    }
    sharksfin::transaction_dispose(tx_);
}

status transaction::commit(bool async) {
    auto rc = sharksfin::transaction_commit(tx_, async);
    if (rc == sharksfin::StatusCode::WAITING_FOR_OTHER_TRANSACTION) {
        VLOG_LP(log_debug) << "commit request has been submitted - waiting for other transaction to finish";
    } else if(rc == sharksfin::StatusCode::OK ||
        rc == sharksfin::StatusCode::ERR_ABORTED ||
        rc == sharksfin::StatusCode::ERR_ABORTED_RETRYABLE) {
        active_ = false;
    }
    return resolve(rc);
}

bool transaction::commit(transaction::commit_callback_type cb) {
    active_ = false;
    return sharksfin::transaction_commit_with_callback(tx_, std::move(cb));
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
    (void) timeout_ns;
    return status::ok;
}

sharksfin::TransactionState transaction::check_state() noexcept {
    sharksfin::TransactionState result{};
    auto rc = sharksfin::transaction_check(tx_, result);
    if(rc != sharksfin::StatusCode::OK) {
        fail();
    }
    return result;
}

status transaction::create_transaction(
    kvs::database &db,
    std::unique_ptr<transaction>& out,
    transaction_option const& options
) {
    auto ret = std::make_unique<transaction>(db);
    if(auto res = ret->init(options); res != status::ok) {
        return res;
    }
    out = std::move(ret);
    return status::ok;
}

template <class T, class E>
bool extract_storages(
    kvs::database* database,
    std::vector<std::unique_ptr<kvs::storage>>& stgs,
    E const& names,
    T& table_areas
) {
    for(auto&& wp : names) {
        auto s = database->get_storage(wp);
        if(! s) {
            VLOG_LP(log_error) << "Specified storage '" << wp << "' is not found.";
            return false;
        }
        table_areas.emplace_back(s->handle());
        stgs.emplace_back(std::move(s));
    }
    return true;
}

status transaction::init(transaction_option const& options) {
    sharksfin::TransactionOptions::WritePreserves wps{};
    sharksfin::TransactionOptions::ReadAreas rai{};
    sharksfin::TransactionOptions::ReadAreas rae{};
    std::vector<std::unique_ptr<kvs::storage>> stgs{}; // to keep storages during transaction_begin call
    stgs.reserve(options.write_preserves().size()+
        options.read_areas_inclusive().size()+options.read_areas_exclusive().size());

    bool success = extract_storages(database_, stgs, options.write_preserves(), wps);
    success = success && extract_storages(database_, stgs, options.read_areas_inclusive(), rai);
    success = success && extract_storages(database_, stgs, options.read_areas_exclusive(), rae);
    if(! success) {
        return status::err_invalid_argument;
    }
    sharksfin::TransactionOptions opts{
        type(options.type()),
        std::move(wps),
        std::move(rai),
        std::move(rae)
    };
    if(auto res = sharksfin::transaction_begin(database_->handle(), opts, &tx_); res != sharksfin::StatusCode::OK) {
        return resolve(res);
    }
    if(auto res = sharksfin::transaction_get_info(tx_, info_); res != sharksfin::StatusCode::OK) {
        return resolve(res);
    }
    active_ = true;
    return status::ok;
}

std::shared_ptr<sharksfin::CallResult> transaction::recent_call_result() noexcept {
    return sharksfin::transaction_inspect_recent_call(tx_);
}

std::string_view transaction::transaction_id() const noexcept {
    if(! info_) return {};
    return info_->id();
}

}

