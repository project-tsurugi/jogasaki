/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <atomic>
#include <utility>
#include <vector>
#include <glog/logging.h>

#include <sharksfin/ErrorCode.h>
#include <sharksfin/StatusCode.h>
#include <sharksfin/TransactionOptions.h>

#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/error.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/kvs/transaction_option.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/status.h>

namespace jogasaki::kvs {

sharksfin::TransactionOptions::TransactionType type(kvs::transaction_option::transaction_type type) {
    using k = kvs::transaction_option::transaction_type;
    using s = sharksfin::TransactionOptions::TransactionType;
    switch (type) {
        case k::occ: return s::SHORT;
        case k::ltx: return s::LONG;
        case k::read_only: return s::READ_ONLY;
    }
    std::abort();
}

transaction::transaction(
    kvs::database &db
) :
    database_(std::addressof(db))
{}

transaction::~transaction() noexcept {
    if (active_) {
        // transaction is not committed or aborted yet.
        // Normally this should not happen, but in order to conform to api contract with cc,
        // we abort the tx here because transaction has been started when this object is created.
        sharksfin::transaction_abort(tx_);
    }
    sharksfin::transaction_dispose(tx_);
}

status transaction::commit(bool async) {
    (void) async;
    active_ = false;
    std::atomic_bool callback_called = false;
    status rc{};
    auto b = sharksfin::transaction_commit_with_callback(tx_, [&](
        ::sharksfin::StatusCode st,
        ::sharksfin::ErrorCode ec,
        ::sharksfin::durability_marker_type marker
    ){
        (void) ec;
        (void) marker;
        callback_called = true;
        rc = resolve(st);
    });
    if(! b) {
        while(! callback_called) {}
    }
    return rc;
}

bool transaction::commit(transaction::commit_callback_type cb) {
    active_ = false;
    return sharksfin::transaction_commit_with_callback(tx_, std::move(cb));
}

status transaction::abort_transaction() {
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

sharksfin::TransactionHandle transaction::handle() {
    if (!handle_) {
        if(auto res = sharksfin::transaction_borrow_handle(tx_, &handle_); res != sharksfin::StatusCode::OK) {
            fail_with_exception();
        }
    }
    return handle_;
}

kvs::database* transaction::database() const noexcept {
    return database_;
}

sharksfin::TransactionState transaction::check_state() {
    sharksfin::TransactionState result{};
    auto rc = sharksfin::transaction_check(tx_, result);
    if(rc != sharksfin::StatusCode::OK) {
        fail_with_exception();
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

