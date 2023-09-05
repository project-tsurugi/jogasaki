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
#include <jogasaki/api/impl/database.h>
#include <sharksfin/api.h>

#include "convert.h"
#include "mapped_record.h"
#include "record_columns.h"
#include "serializer.h"
#include "transaction_utils.h"

using takatori::util::throw_exception;

namespace jogasaki::api::kvsservice {

transaction::transaction(jogasaki::api::database *db,
                         sharksfin::TransactionControlHandle handle) :
    db_(dynamic_cast<jogasaki::api::impl::database *>(db)), ctrl_handle_(handle) {
    if (handle != nullptr) {
        auto status = sharksfin::transaction_borrow_handle(handle, &tx_handle_);
        if (status != sharksfin::StatusCode::OK) {
            throw_exception(std::logic_error{"transaction_borrow_handle failed"});
        }
    } else {
        throw_exception(std::logic_error{"TransactionControlHandle is null"});
    }
    if (auto code = sharksfin::transaction_borrow_owner(tx_handle_, &db_handle_);
            code != sharksfin::StatusCode::OK) {
        throw_exception(std::logic_error{"transaction_borrow_owner failed"});
    }
    system_id_ = (std::uint64_t)(this); // NOLINT
}

std::uint64_t transaction::system_id() const noexcept {
    return system_id_;
}

transaction_state transaction::state() const {
    transaction_state::state_kind kind{};
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
    // FIXME
    auto status_d = sharksfin::transaction_dispose(ctrl_handle_);
    return convert(status_c, status_d);
}

status transaction::abort() {
    auto status_a = sharksfin::transaction_abort(ctrl_handle_);
    if (status_a != sharksfin::StatusCode::OK) {
        return convert(status_a);
    }
    auto status_d = sharksfin::transaction_dispose(ctrl_handle_);
    return convert(status_a, status_d);
}

status transaction::get_storage(std::string_view name, sharksfin::StorageHandle &storage) {
    sharksfin::Slice key {name};
    auto code = sharksfin::storage_get(db_handle_, key, &storage);
    return convert(code);
}

status transaction::put(std::string_view table_name, tateyama::proto::kvs::data::Record const &record,
                        put_option opt) {
    if (!is_valid_record(record)) {
        return status::err_invalid_argument;
    }
    std::shared_ptr<yugawara::storage::table const> table{};
    if (auto s = get_table(db_, table_name, table); s != status::ok) {
        return s;
    }
    record_columns rec_cols{table, record, false};
    if (auto s = check_put_record(rec_cols); s != status::ok) {
        return s;
    }
    auto key_size = get_bufsize(spec_primary_key, nullable_primary_key, rec_cols.primary_keys());
    jogasaki::data::aligned_buffer key_buffer{key_size};
    jogasaki::kvs::writable_stream key_stream{key_buffer.data(), key_buffer.capacity()};
    if (auto s = serialize(spec_primary_key, nullable_primary_key, rec_cols.primary_keys(), key_stream); s != status::ok) {
        return s;
    }
    auto value_size = get_bufsize(spec_value, nullable_value, rec_cols.values());
    jogasaki::data::aligned_buffer value_buffer{value_size};
    jogasaki::kvs::writable_stream value_stream{value_buffer.data(), value_buffer.capacity()};
    if (auto s = serialize(spec_value, nullable_value, rec_cols.values(), value_stream); s != status::ok) {
        return s;
    }
    sharksfin::Slice key_slice {key_stream.data(), key_stream.size()};
    sharksfin::Slice value_slice {value_stream.data(), value_stream.size()};
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(table_name, storage); s != status::ok) {
        return s;
    }
    auto option = convert(opt);
    auto code = sharksfin::content_put(tx_handle_, storage, key_slice, value_slice, option);
    auto code2 = sharksfin::storage_dispose(storage);
    return convert(code, code2);
}

status transaction::get(std::string_view table_name, tateyama::proto::kvs::data::Record const &primary_key,
                        tateyama::proto::kvs::data::Record &record) {
    if (!is_valid_record(primary_key)) {
        return status::err_invalid_argument;
    }
    std::shared_ptr<yugawara::storage::table const> table{};
    if (auto s = get_table(db_, table_name, table); s != status::ok) {
        return s;
    }
    record_columns rec_cols{table, primary_key, true};
    if (auto s = check_valid_primary_key(rec_cols); s != status::ok) {
        return s;
    }
    auto key_size = get_bufsize(spec_primary_key, nullable_primary_key, rec_cols.primary_keys());
    jogasaki::data::aligned_buffer key_buffer{key_size};
    jogasaki::kvs::writable_stream key_stream{key_buffer.data(), key_buffer.capacity()};
    if (auto s = serialize(spec_primary_key, nullable_primary_key, rec_cols.primary_keys(), key_stream);
        s != status::ok) {
        return s;
    }
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(table_name, storage); s != status::ok) {
        return s;
    }
    sharksfin::Slice key_slice {key_stream.data(), key_stream.size()};
    sharksfin::Slice value_slice{};
    auto code = sharksfin::content_get(tx_handle_, storage, key_slice, &value_slice);
    auto code2 = sharksfin::storage_dispose(storage);
    if (code != sharksfin::StatusCode::OK || code2 != sharksfin::StatusCode::OK) {
        return convert(code, code2);
    }
    return make_record(table, primary_key, value_slice, record);
}

status transaction::remove(std::string_view table_name, tateyama::proto::kvs::data::Record const &primary_key,
                        remove_option opt) {
    if (!is_valid_record(primary_key)) {
        return status::err_invalid_argument;
    }
    std::shared_ptr<yugawara::storage::table const> table{};
    if (auto s = get_table(db_, table_name, table); s != status::ok) {
        return s;
    }
    record_columns rec_cols{table, primary_key, true};
    if (auto s = check_valid_primary_key(rec_cols); s != status::ok) {
        return s;
    }
    auto key_size = get_bufsize(spec_primary_key, nullable_primary_key, rec_cols.primary_keys());
    jogasaki::data::aligned_buffer key_buffer{key_size};
    jogasaki::kvs::writable_stream key_stream{key_buffer.data(), key_buffer.capacity()};
    if (auto s = serialize(spec_primary_key, nullable_primary_key, rec_cols.primary_keys(), key_stream);
        s != status::ok) {
        return s;
    }
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(table_name, storage); s != status::ok) {
        return s;
    }
    sharksfin::Slice key_slice {key_stream.data(), key_stream.size()};
    if (opt == remove_option::counting) {
        auto code = sharksfin::content_check_exist(tx_handle_, storage, key_slice);
        if (code != sharksfin::StatusCode::OK) {
            // NOT_FOUND, or error
            auto code2 = sharksfin::storage_dispose(storage);
            return convert(code, code2);
        }
    }
    auto code = sharksfin::content_delete(tx_handle_, storage, key_slice);
    if (opt == remove_option::instant && code == sharksfin::StatusCode::NOT_FOUND) {
        code = sharksfin::StatusCode::OK;
    }
    auto code2 = sharksfin::storage_dispose(storage);
    return convert(code, code2);
}

}
