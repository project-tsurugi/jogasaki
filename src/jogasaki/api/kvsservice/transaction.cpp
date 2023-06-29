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

static status get_storage(sharksfin::TransactionHandle &tx, std::string_view name, sharksfin::StorageHandle &storage) {
    // NOTE  sharksfin::storage_get(tx, key, &storage) returns NOT_IMPLEMENTED
    sharksfin::DatabaseHandle db{};
    if (auto code = sharksfin::transaction_borrow_owner(tx, &db);
            code != sharksfin::StatusCode::OK) {
        return convert(code);
    }
    sharksfin::Slice key {name};
    auto code = sharksfin::storage_get(db, key, &storage);
    return convert(code);
}

static sharksfin::PutOperation convert(put_option opt) {
    switch (opt) {
        case put_option::create_or_update:
            return sharksfin::PutOperation::CREATE_OR_UPDATE;
        case put_option::create:
            return sharksfin::PutOperation::CREATE;
        case put_option::update:
            return sharksfin::PutOperation::UPDATE;
        default:
            throw_exception(std::logic_error{"unknown put_option"});
    }
}

status transaction::put(std::string_view table, tateyama::proto::kvs::data::Record const &record,
                        put_option opt) {
    {
        // FIXME
        if (record.names_size() != 2 || record.values_size() != 2) {
            return status::err_unsupported;
        }
        if (record.names(0) != "key") {
            return status::err_unsupported;
        }
    }
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(tx_handle_, table, storage);
        s != status::ok) {
        return s;
    }
    // FIXME
    auto key = record.values(0).int8_value();
    auto keyS = sharksfin::Slice(&key, sizeof(key));
    bool exists = false;
    if (opt != put_option::create_or_update) {
        auto code = sharksfin::content_check_exist(tx_handle_, storage, keyS);
        exists = code == sharksfin::StatusCode::OK;
    }
    // FIXME
    auto value = record.values(1).int8_value();
    auto valueS = sharksfin::Slice(&value, sizeof(value));
    auto option = convert(opt);
    auto code = sharksfin::content_put(tx_handle_, storage, keyS, valueS, option);
    auto code_d = sharksfin::storage_dispose(storage);
    if (code == sharksfin::StatusCode::OK) {
        switch (opt) {
            case put_option::create:
                if (exists) {
                    code = sharksfin::StatusCode::ALREADY_EXISTS;
                }
                break;
            case put_option::update:
                if (!exists) {
                    code = sharksfin::StatusCode::NOT_FOUND;
                }
                break;
            default:
                break;
        }

    }
    return convert(code, code_d);
}

status transaction::get(std::string_view table, tateyama::proto::kvs::data::Record const &primary_key,
                        tateyama::proto::kvs::data::Record &record) {
    {
        // FIXME
        if (primary_key.names_size() == 0) {
            return status::err_invalid_key_length;
        }
        if (primary_key.names(0) != "key") {
            return status::err_unsupported;
        }
    }
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(tx_handle_, table, storage);
            s != status::ok) {
        return s;
    }
    // FIXME
    auto key = primary_key.values(0).int8_value();
    auto keyS = sharksfin::Slice(&key, sizeof(key));
    sharksfin::Slice valueS{};
    auto code = sharksfin::content_get(tx_handle_, storage, keyS, &valueS);
    if (code == sharksfin::StatusCode::OK) {
        if (valueS.size() == sizeof(long)) {
            // FIXME
            record.add_names(primary_key.names(0));
            record.add_names("value0");
            {
                tateyama::proto::kvs::data::Value *value = new tateyama::proto::kvs::data::Value();
                value->set_int8_value(key);
                record.mutable_values()->AddAllocated(value);
            }
            {
                long v = *(long *) valueS.data();
                tateyama::proto::kvs::data::Value *value = new tateyama::proto::kvs::data::Value();
                value->set_int8_value(v);
                record.mutable_values()->AddAllocated(value);
            }
        } else {
            code = sharksfin::StatusCode::ERR_INVALID_ARGUMENT;
        }
    }
    auto code_d = sharksfin::storage_dispose(storage);
    return convert(code, code_d);
    return status::ok;
}

status transaction::remove(std::string_view table, tateyama::proto::kvs::data::Record const &primary_key,
                        remove_option opt) {
    {
        // FIXME
        if (primary_key.names_size() == 0) {
            return status::err_invalid_key_length;
        }
        if (primary_key.names(0) != "key") {
            return status::err_unsupported;
        }
    }
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(tx_handle_, table, storage);
            s != status::ok) {
        return s;
    }
    auto key = primary_key.values(0).int8_value();
    auto keyS = sharksfin::Slice(&key, sizeof(key));
    if (opt == remove_option::counting) {
        auto code = sharksfin::content_check_exist(tx_handle_, storage, keyS);
        switch (code) {
            case sharksfin::StatusCode::OK:
                break;
            case sharksfin::StatusCode::NOT_FOUND:
                return status::not_found;
            default:
                return convert(code);
        }
    }
    // FIXME
    auto code = sharksfin::content_delete(tx_handle_, storage, keyS);
    auto code_d = sharksfin::storage_dispose(storage);
    return convert(code, code_d);
}

}

