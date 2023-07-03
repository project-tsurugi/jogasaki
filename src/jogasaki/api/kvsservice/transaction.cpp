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
    return status::err_unknown;
}

static status check_put_record(jogasaki::api::impl::database* db_,
                               std::string_view table_name,
                               tateyama::proto::kvs::data::Record const &record) {
    std::shared_ptr<yugawara::storage::table const> table {};
    if (auto s = get_table(db_, table_name, table); s != status::ok) {
        return s;
    }
    // TODO support default values (currently all columns' values are necessary)
    auto columns = table->columns();
    if (columns.size() != static_cast<unsigned long>(record.names_size())) {
        return status::err_invalid_argument; // FIXME
    }
    mapped_record m_rec{record};
    for (auto &col : columns) {
        // TODO should be case-insensitive
        auto value = m_rec.get_value(col.simple_name());
        if (value == nullptr) {
            // unknown column name
            return status::err_invalid_argument; // FIXME
        }
        if (!equal_type(col.type().kind(), value->value_case())) {
            // invalid data type
            return status::err_invalid_argument; // FIXME
        }
    }
    return status::ok;
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
    if (auto s = check_put_record(db_, table, record);
        s != status::ok) {
        return s;
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

static status check_primary_key(jogasaki::api::impl::database* db_,
                                std::string_view table_name,
                                tateyama::proto::kvs::data::Record const &primary_key,
                                std::vector<tateyama::proto::kvs::data::Value const*> &key_values) {
    std::shared_ptr<yugawara::storage::table const> table {};
    if (auto s = get_table(db_, table_name, table); s != status::ok) {
        return s;
    }
    auto primary = table->owner()->find_primary_index(*table.get());
    if (primary == nullptr) {
        return status::err_invalid_argument;
    }
    auto keys = primary->keys();
    if (keys.size() != static_cast<std::size_t>(primary_key.names_size())) {
        return status::err_invalid_key_length;
    }
    mapped_record m_key{primary_key};
    for (auto &key : keys) {
        auto &col = key.column();
        auto value = m_key.get_value(col.simple_name());
        if (value == nullptr) {
            // unknown column name
            return status::err_invalid_argument; // FIXME
        }
        if (!equal_type(col.type().kind(), value->value_case())) {
            // invalid data type
            return status::err_invalid_argument; // FIXME
        }
        key_values.emplace_back(value);
    }
    return status::ok;
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
    std::vector<tateyama::proto::kvs::data::Value const*> key_values{};
    if (auto s = check_primary_key(db_, table, primary_key, key_values);
        s != status::ok) {
        return s;
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
    std::vector<tateyama::proto::kvs::data::Value const*> key_values{};
    if (auto s = check_primary_key(db_, table, primary_key, key_values);
            s != status::ok) {
        return s;
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
