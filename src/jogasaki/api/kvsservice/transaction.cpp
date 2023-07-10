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

#include <unordered_set>

#include <jogasaki/api/kvsservice/transaction.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/serializer/value_input.h>
#include <sharksfin/api.h>

#include "convert.h"
#include "mapped_record.h"
#include "serializer.h"

using takatori::util::throw_exception;
using cbuffer = takatori::util::const_buffer_view;

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
    sharksfin::Slice key {name};
    auto code = sharksfin::storage_get(db_handle_, key, &storage);
    return convert(code);
}

static bool equal_type(takatori::type::type_kind kind,
                       tateyama::proto::kvs::data::Value::ValueCase val_case) {
    switch (kind) {
        case takatori::type::type_kind::boolean:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kBooleanValue;
        case takatori::type::type_kind::int1:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kInt4Value; // FIXME
        case takatori::type::type_kind::int2:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kInt4Value; // FIXME
        case takatori::type::type_kind::int4:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kInt4Value;
        case takatori::type::type_kind::int8:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kInt8Value;
        case takatori::type::type_kind::float4:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kFloat4Value;
        case takatori::type::type_kind::float8:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kFloat8Value;
        case takatori::type::type_kind::decimal:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kDecimalValue;
        case takatori::type::type_kind::character:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kCharacterValue;
        case takatori::type::type_kind::octet:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kOctetValue;
        case takatori::type::type_kind::bit:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kOctetValue; // FIXME
        case takatori::type::type_kind::date:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kDateValue;
        case takatori::type::type_kind::time_of_day:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kTimeOfDayValue;
        case takatori::type::type_kind::time_point:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kTimePointValue;
        case takatori::type::type_kind::datetime_interval:
            return val_case == tateyama::proto::kvs::data::Value::ValueCase::kDatetimeIntervalValue;
        default:
            takatori::util::throw_exception(std::logic_error{"unknown type_kind"});
    }
}

static status get_table(jogasaki::api::impl::database* db_,
                        std::string_view table_name,
                        std::shared_ptr<yugawara::storage::table const> &table) {
    if (table_name.empty()) {
        return status::err_invalid_argument;
    }
    table = db_->tables()->find_table(table_name);
    if (table != nullptr) {
        return status::ok;
    }
    return status::err_table_not_found;
}

static status make_primary_key_names(std::shared_ptr<yugawara::storage::table const> &table,
                                   std::unordered_set<std::string_view> &primary_key_names) {
    const auto primary = table->owner()->find_primary_index(*table);
    if (primary == nullptr) {
        return status::err_invalid_argument;
    }
    for (auto & key : primary->keys()) {
        primary_key_names.emplace(key.column().simple_name());
    }
    return status::ok;
}

static bool is_valid_record(tateyama::proto::kvs::data::Record const &record) {
    return record.names_size() >= 1 && record.names_size() == record.values_size();
}

static status check_put_record(std::shared_ptr<yugawara::storage::table const> &table,
                               tateyama::proto::kvs::data::Record const &record,
                               std::vector<tateyama::proto::kvs::data::Value const*> &key_values,
                               std::vector<tateyama::proto::kvs::data::Value const*> &value_values) {
    if (!is_valid_record(record)) {
        return status::err_invalid_argument;
    }
    // TODO support default values (currently all columns' values are necessary)
    const auto columns = table->columns();
    auto col_size = columns.size();
    decltype(col_size) rec_size = static_cast<decltype(col_size)>(record.names_size());
    if (rec_size < col_size) {
        return status::err_incomplete_columns;
    } else if (rec_size > col_size) {
        return status::err_mismatch_key;
    }
    std::unordered_set<std::string_view> primary_key_names{};
    if (auto s = make_primary_key_names(table, primary_key_names);
        s != status::ok) {
        return s;
    }
    mapped_record m_rec{record};
    for (auto &col : columns) {
        auto col_name = col.simple_name();
        // TODO should be case-insensitive
        auto value = m_rec.get_value(col_name);
        if (value == nullptr) {
            return status::err_column_not_found;
        }
        if (!equal_type(col.type().kind(), value->value_case())) {
            return status::err_column_type_mismatch;
        }
        if (primary_key_names.find(col_name) != primary_key_names.cend()) {
            key_values.emplace_back(value);
        } else {
            value_values.emplace_back(value);
        }
    }
    return status::ok;
}

static std::size_t calc_max_bufsize(std::vector<tateyama::proto::kvs::data::Value const*> &values) {
    std::size_t len = 0;
    for (auto value: values) {
        len += 1; // for type data
        switch (value->value_case()) {
            case tateyama::proto::kvs::data::Value::ValueCase::kBooleanValue:
                len += 4;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kInt4Value:
                len += 4;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kInt8Value:
                len += 8;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat4Value:
                len += 4;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kFloat8Value:
                len += 8;
                break;
            case tateyama::proto::kvs::data::Value::ValueCase::kCharacterValue:
                // string length (8 bytes) + string data
                len += 8 + value->character_value().size();
                break;
            default:
                takatori::util::throw_exception(std::logic_error{"not implemented: unknown value_case"});
                break;
        }
    }
    return len;
}

status transaction::put(std::string_view table_name, tateyama::proto::kvs::data::Record const &record,
                        put_option opt) {
    std::shared_ptr<yugawara::storage::table const> table{};
    if (auto s = get_table(db_, table_name, table); s != status::ok) {
        return s;
    }
    std::vector<tateyama::proto::kvs::data::Value const*> key_values{};
    std::vector<tateyama::proto::kvs::data::Value const*> value_values{};
    if (auto s = check_put_record(table, record, key_values, value_values);
        s != status::ok) {
        return s;
    }
    jogasaki::data::aligned_buffer key_buffer{calc_max_bufsize(key_values)};
    jogasaki::kvs::writable_stream key_stream{key_buffer.data(), key_buffer.capacity()};
    if (auto s = serialize(jogasaki::kvs::spec_key_ascending, false, key_values, key_stream); s != status::ok) {
        return s;
    }
    jogasaki::data::aligned_buffer value_buffer{calc_max_bufsize(value_values)};
    jogasaki::kvs::writable_stream value_stream{value_buffer.data(), value_buffer.capacity()};
    if (auto s = serialize(jogasaki::kvs::spec_value, true, value_values, value_stream); s != status::ok) {
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

static status check_primary_key(std::shared_ptr<yugawara::storage::table const> &table,
                                tateyama::proto::kvs::data::Record const &primary_key,
                                std::vector<tateyama::proto::kvs::data::Value const*> &key_values) {
    if (!is_valid_record(primary_key)) {
        return status::err_invalid_argument;
    }
    const auto primary = table->owner()->find_primary_index(*table);
    if (primary == nullptr) {
        return status::err_invalid_argument;
    }
    const auto keys = primary->keys();
    auto key_size = keys.size();
    decltype(key_size) req_size = static_cast<decltype(key_size)>(primary_key.names_size());
    if (key_size != req_size) {
        return status::err_mismatch_key;
    }
    mapped_record m_key{primary_key};
    for (const auto &key : keys) {
        auto &col = key.column();
        auto value = m_key.get_value(col.simple_name());
        if (value == nullptr) {
            return status::err_column_not_found;
        }
        if (!equal_type(col.type().kind(), value->value_case())) {
            return status::err_column_type_mismatch;
        }
        key_values.emplace_back(value);
    }
    return status::ok;
}

static void add_column(std::string_view col_name,
                       tateyama::proto::kvs::data::Value const *value,
                       tateyama::proto::kvs::data::Record &record) {
    record.add_names(col_name.data());
    auto new_value = new tateyama::proto::kvs::data::Value(*value);
    record.mutable_values()->AddAllocated(new_value);
}

static void add_column(yugawara::storage::column const &column,
                       jogasaki::kvs::readable_stream &stream,
                       tateyama::proto::kvs::data::Record &record) {
    record.add_names(column.simple_name().data());
    auto new_value = new tateyama::proto::kvs::data::Value();
    deserialize(jogasaki::kvs::spec_value, true, column.type(), stream, new_value);
    record.mutable_values()->AddAllocated(new_value);
}

static void make_record(std::shared_ptr<yugawara::storage::table const> &table,
                    tateyama::proto::kvs::data::Record const &primary_key,
                sharksfin::Slice const &value_slice,
                tateyama::proto::kvs::data::Record &record) {
    auto input = value_slice.to_string_view();
    jogasaki::kvs::readable_stream stream{input.data(), input.size()};
    //
    mapped_record m_key {primary_key};
    for (auto &col : table->columns()) {
        auto col_name = col.simple_name();
        auto value = m_key.get_value(col_name);
        if (value != nullptr) {
            add_column(col_name, value, record);
        } else {
            add_column(col, stream, record);
        }
    }
}

status transaction::get(std::string_view table_name, tateyama::proto::kvs::data::Record const &primary_key,
                        tateyama::proto::kvs::data::Record &record) {
    std::shared_ptr<yugawara::storage::table const> table {};
    if (auto s = get_table(db_, table_name, table); s != status::ok) {
        return s;
    }
    std::vector<tateyama::proto::kvs::data::Value const*> key_values{};
    if (auto s = check_primary_key(table, primary_key, key_values);
        s != status::ok) {
        return s;
    }
    jogasaki::data::aligned_buffer key_buffer{calc_max_bufsize(key_values)};
    jogasaki::kvs::writable_stream key_stream{key_buffer.data(), key_buffer.capacity()};
    if (auto s = serialize(jogasaki::kvs::spec_key_ascending, false, key_values, key_stream); s != status::ok) {
        return s;
    }
    sharksfin::StorageHandle storage{};
    if (auto s = get_storage(table_name, storage); s != status::ok) {
        return s;
    }
    sharksfin::Slice key_slice {key_stream.data(), key_stream.size()};
    sharksfin::Slice value_slice{};
    auto code = sharksfin::content_get(tx_handle_, storage, key_slice, &value_slice);
    if (code == sharksfin::StatusCode::OK) {
        make_record(table, primary_key, value_slice, record);
    }
    auto code2 = sharksfin::storage_dispose(storage);
    return convert(code, code2);
}

status transaction::remove(std::string_view table_name, tateyama::proto::kvs::data::Record const &primary_key,
                        remove_option opt) {
    std::shared_ptr<yugawara::storage::table const> table {};
    if (auto s = get_table(db_, table_name, table); s != status::ok) {
        return s;
    }
    std::vector<tateyama::proto::kvs::data::Value const*> key_values{};
    if (auto s = check_primary_key(table, primary_key, key_values);
            s != status::ok) {
        return s;
    }
    jogasaki::data::aligned_buffer key_buffer{calc_max_bufsize(key_values)};
    jogasaki::kvs::writable_stream key_stream{key_buffer.data(), key_buffer.capacity()};
    if (auto s = serialize(jogasaki::kvs::spec_key_ascending, false, key_values, key_stream); s != status::ok) {
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
