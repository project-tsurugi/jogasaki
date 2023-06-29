/*
 * Copyright 2018-2023 tsurugi project.
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

#include <jogasaki/api/kvsservice/transaction.h>
#include <sharksfin/api.h>

#include "convert.h"

namespace jogasaki::api::kvsservice {

transaction::transaction(sharksfin::TransactionControlHandle handle) : ctrl_handle_(handle) {
    if (handle != nullptr) {
        auto status = sharksfin::transaction_borrow_handle(handle, &tx_handle_);
        if (status != sharksfin::StatusCode::OK) {
            throw_exception(std::logic_error{"transaction_borrow_handle failed"});
        }
    } else {
        throw_exception(std::logic_error{"TransactionControlHandle is null"});
    }
    system_id_ = (std::uint64_t)(this); // NOLINT
}

std::uint64_t transaction::system_id() const noexcept {
    return system_id_;
}

static transaction_state::state_kind convert(sharksfin::TransactionState::StateKind kind) {
    switch (kind) {
        case sharksfin::TransactionState::StateKind::UNKNOWN:
            return transaction_state::state_kind::unknown;
        case sharksfin::TransactionState::StateKind::WAITING_START:
            return transaction_state::state_kind::waiting_start;
        case sharksfin::TransactionState::StateKind::STARTED:
            return transaction_state::state_kind::started;
        case sharksfin::TransactionState::StateKind::WAITING_CC_COMMIT:
            return transaction_state::state_kind::waiting_cc_commit;
        case sharksfin::TransactionState::StateKind::ABORTED:
            return transaction_state::state_kind::aborted;
        case sharksfin::TransactionState::StateKind::WAITING_DURABLE:
            return transaction_state::state_kind::waiting_durable;
        case sharksfin::TransactionState::StateKind::DURABLE:
            return transaction_state::state_kind::durable;
        default:
            throw_exception(std::logic_error{"unknown kind"});
    }
}

transaction_state transaction::state() const {
    sharksfin::TransactionState state;
    auto status = sharksfin::transaction_check(ctrl_handle_, state);
    transaction_state::state_kind kind;
    if (status == sharksfin::StatusCode::OK) {
        kind = convert(state.state_kind());
    } else {
        kind = transaction_state::state_kind::unknown;
    }
    return transaction_state(kind);
}

std::mutex &transaction::transaction_mutex() {
    return mtx_tx_;
}

status transaction::commit() {
    auto status_c = sharksfin::transaction_commit(ctrl_handle_);
    if (status_c != sharksfin::StatusCode::OK) {
        return convert(status_c);
    }
    // NOTE sharksfin::transaction_check() blocks after transaction_commit() ???
//    if (state().kind() == transaction_state::state_kind::durable) {
        // FIXME
        auto status_d = sharksfin::transaction_dispose(ctrl_handle_);
        return convert(status_c, status_d);
//    }
//    return convert(status_c);
}

status transaction::abort() {
    auto status_a = sharksfin::transaction_abort(ctrl_handle_);
    if (status_a != sharksfin::StatusCode::OK) {
        return convert(status_a);
    }
//    if (state().kind() == transaction_state::state_kind::aborted) {
        // FIXME
        auto status_d = sharksfin::transaction_dispose(ctrl_handle_);
        return convert(status_a, status_d);
//    }
//    return convert(status_a);
}

status transaction::put(std::string_view, tateyama::proto::kvs::data::Record const &,
                        put_option opt) {
    switch (opt) {
        case put_option::create_or_update:
            break;
        case put_option::create:
        case put_option::update:
            break;
        default:
            throw_exception(std::logic_error{"unknown put_option"});
    }
    return status::ok;
}

status transaction::get(std::string_view, tateyama::proto::kvs::data::Record const &primary_key,
                        tateyama::proto::kvs::data::Record &record) {
    // FIXME call sharksfin
    record.CopyFrom(primary_key);
    return status::ok;
}

status transaction::remove(std::string_view, tateyama::proto::kvs::data::Record const &,
                        remove_option) {
    // FIXME call sharksfin
    return status::ok;
}

}

