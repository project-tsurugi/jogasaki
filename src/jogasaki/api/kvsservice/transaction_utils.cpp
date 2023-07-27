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

#include "mapped_record.h"
#include "serializer.h"
#include "transaction_utils.h"

using takatori::util::throw_exception;

namespace jogasaki::api::kvsservice {

bool equal_type(takatori::type::type_kind kind,
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

status get_table(jogasaki::api::impl::database* db,
                        std::string_view table_name,
                        std::shared_ptr<yugawara::storage::table const> &table) {
    if (table_name.empty()) {
        return status::err_invalid_argument;
    }
    table = db->tables()->find_table(table_name);
    if (table != nullptr) {
        return status::ok;
    }
    return status::err_table_not_found;
}

status make_primary_key_names(std::shared_ptr<yugawara::storage::table const> &table,
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

bool is_valid_record(tateyama::proto::kvs::data::Record const &record) {
    return record.names_size() >= 1 && record.names_size() == record.values_size();
}

status check_put_record(std::shared_ptr<yugawara::storage::table const> &table,
                               tateyama::proto::kvs::data::Record const &record,
                               std::vector<tateyama::proto::kvs::data::Value const*> &key_values,
                               std::vector<tateyama::proto::kvs::data::Value const*> &value_values) {
    if (!is_valid_record(record)) {
        return status::err_invalid_argument;
    }
    // TODO support default values (currently all columns' values are necessary)
    const auto columns = table->columns();
    auto col_size = columns.size();
    auto rec_size = static_cast<decltype(col_size)>(record.names_size());
    if (rec_size < col_size) {
        return status::err_incomplete_columns;
    }
    if (rec_size > col_size) {
        return status::err_invalid_argument;
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

status check_primary_key(std::shared_ptr<yugawara::storage::table const> &table,
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
    auto req_size = static_cast<decltype(key_size)>(primary_key.names_size());
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

void add_key_column(std::string_view col_name,
                       tateyama::proto::kvs::data::Value const *value,
                       tateyama::proto::kvs::data::Record &record) {
    record.add_names(col_name.data());
    auto new_value = new tateyama::proto::kvs::data::Value(*value);
    record.mutable_values()->AddAllocated(new_value);
}

status add_value_column(yugawara::storage::column const &column,
                         jogasaki::kvs::readable_stream &stream,
                         tateyama::proto::kvs::data::Record &record) {
    record.add_names(column.simple_name().data());
    auto new_value = new tateyama::proto::kvs::data::Value();
    if (auto s = deserialize(spec_value, nullable_value,
                             column.type().kind(), stream, new_value);
            s != status::ok) {
        delete new_value;
        return s;
    }
    record.mutable_values()->AddAllocated(new_value);
    return status::ok;
}

status make_record(std::shared_ptr<yugawara::storage::table const> &table,
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
            add_key_column(col_name, value, record);
        } else if (auto s = add_value_column(col, stream, record);
                s != status::ok) {
            return s;
        }
    }
    return status::ok;
}

}
