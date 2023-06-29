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
using takatori::util::throw_exception;

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
    if (auto code = sharksfin::transaction_borrow_owner(tx_handle_, &db_);
            code != sharksfin::StatusCode::OK) {
        throw_exception(std::logic_error{"transaction_borrow_owner failed"});
    }
    system_id_ = (std::uint64_t)(this); // NOLINT
}

transaction::~transaction() {
    for (auto &it : storage_map_) {
        auto storage = it.second;
        sharksfin::storage_dispose(storage);
        // FIXME error log
    }
}

std::uint64_t transaction::system_id() const noexcept {
    return system_id_;
}

transaction_state transaction::state() const {
    transaction_state::state_kind kind;
    sharksfin::TransactionState state;
    auto status = sharksfin::transaction_check(ctrl_handle_, state);
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

status transaction::get_storage(std::string_view name, sharksfin::StorageHandle &storage) {
    storage = storage_map_[std::string{name}];
    if (storage != nullptr) {
        return status::ok;
    }
    sharksfin::Slice key {name};
    auto code = sharksfin::storage_get(db_, key, &storage);
    if (code == sharksfin::StatusCode::OK) {
        storage_map_[std::string{name}] = storage;
    }
    return convert(code);
}

static sharksfin::StatusCode put_code(sharksfin::StatusCode code, put_option opt, bool exists) {
    if (code != sharksfin::StatusCode::OK) {
        return code;
    }
    switch (opt) {
        case put_option::create:
            if (exists) {
                return sharksfin::StatusCode::ALREADY_EXISTS;
            }
            break;
        case put_option::update:
            if (!exists) {
                return sharksfin::StatusCode::NOT_FOUND;
            }
            break;
        default:
            break;
    }
    return code;
}

status transaction::put(std::string_view table, tateyama::proto::kvs::data::Record const &record,
                        put_option opt) {
    {
        // FIXME
        if (record.names_size() != 2 || record.values_size() != 2) {
            return status::err_unsupported;
        }
    }
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(table, storage);
        s != status::ok) {
        return s;
    }
    // FIXME
    auto key = record.values(0).int8_value();
    auto keyS = sharksfin::Slice(&key, sizeof(key));
    bool exists {};
    if (opt != put_option::create_or_update) {
        auto code = sharksfin::content_check_exist(tx_handle_, storage, keyS);
        exists = code == sharksfin::StatusCode::OK;
    }
    // FIXME
    auto value = record.values(1).int8_value();
    auto valueS = sharksfin::Slice(&value, sizeof(value));
    auto option = convert(opt);
    auto code = sharksfin::content_put(tx_handle_, storage, keyS, valueS, option);
    code = put_code(code, opt, exists);
    return convert(code);
}

static void make_record(tateyama::proto::kvs::data::Record const &primary_key,
                        sharksfin::Slice const &valueS,
                        tateyama::proto::kvs::data::Record &record) {
    // FIXME
    record.add_names(primary_key.names(0));
    record.add_names("value0");
    {
        auto value = new tateyama::proto::kvs::data::Value();
        value->set_int8_value(primary_key.values(0).int8_value());
        record.mutable_values()->AddAllocated(value);
    }
    {
        auto value = new tateyama::proto::kvs::data::Value();
        auto v = *(google::protobuf::int64 *) valueS.data();
        value->set_int8_value(v);
        record.mutable_values()->AddAllocated(value);
    }
}

status transaction::get(std::string_view table, tateyama::proto::kvs::data::Record const &primary_key,
                        tateyama::proto::kvs::data::Record &record) {
    {
        // FIXME
        if (primary_key.names_size() == 0) {
            return status::err_invalid_key_length;
        }
    }
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(table, storage);
            s != status::ok) {
        return s;
    }
    // FIXME
    auto key = primary_key.values(0).int8_value();
    auto keyS = sharksfin::Slice(&key, sizeof(key));
    sharksfin::Slice valueS{};
    auto code = sharksfin::content_get(tx_handle_, storage, keyS, &valueS);
    if (code == sharksfin::StatusCode::OK) {
        // FIXME
        if (valueS.size() == sizeof(google::protobuf::int64)) {
            make_record(primary_key, valueS, record);
        } else {
            // FIXME
            code = sharksfin::StatusCode::ERR_INVALID_ARGUMENT;
        }
    }
    return convert(code);
}

status transaction::remove(std::string_view table, tateyama::proto::kvs::data::Record const &primary_key,
                        remove_option opt) {
    {
        // FIXME
        if (primary_key.names_size() == 0) {
            return status::err_invalid_key_length;
        }
    }
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(table, storage);
            s != status::ok) {
        return s;
    }
    auto key = primary_key.values(0).int8_value();
    auto keyS = sharksfin::Slice(&key, sizeof(key));
    if (opt == remove_option::counting) {
        auto code = sharksfin::content_check_exist(tx_handle_, storage, keyS);
        if (code != sharksfin::StatusCode::OK) {
            // NOT_FOUND, or error
            return convert(code);
        }
    }
    // FIXME
    auto code = sharksfin::content_delete(tx_handle_, storage, keyS);
    if (opt == remove_option::instant && code == sharksfin::StatusCode::NOT_FOUND) {
        code = sharksfin::StatusCode::OK;
    }
    return convert(code);
}

}
