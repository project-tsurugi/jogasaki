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

#include <jogasaki/utils/storage_utils.h>

#include "mapped_record.h"
#include "serializer.h"
#include "transaction_utils.h"

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

bool has_secondary_index(std::shared_ptr<yugawara::storage::table const> &table) {
    auto size = jogasaki::utils::index_count(*table);
    return size > 1;
}

bool is_valid_record(tateyama::proto::kvs::data::Record const &record) noexcept {
    return record.names_size() >= 1 && record.names_size() == record.values_size();
}

static status check_valid_reccols(record_columns &rec_cols) noexcept {
    if (rec_cols.has_unknown_column()) {
        return status::err_column_not_found;
    }
    if (rec_cols.has_duplicate_column()) {
        return status::err_invalid_argument;
    }
    return status::ok;
}

static status check_valid_column(column_data const &cd) {
    if (cd.value() == nullptr) {
        // TODO support default values (currently all columns' values are necessary)
        return status::err_incomplete_columns;
    }
    if (cd.value()->value_case() == tateyama::proto::kvs::data::Value::VALUE_NOT_SET) {
        if (cd.column()->criteria().nullity().nullable()) {
            return status::ok;
        }
        return status::err_invalid_argument;
    }
    if (!equal_type(cd.column()->type().kind(), cd.value()->value_case())) {
        return status::err_column_type_mismatch;
    }
    return status::ok;
}

static status check_valid_columns(std::vector<column_data> const &cd_list) {
    for (const auto &cd : cd_list) {
        if (auto s = check_valid_column(cd); s != status::ok) {
            return s;
        }
    }
    return status::ok;
}

static status check_valid_key_size(record_columns &rec_cols, std::size_t key_size) {
    if (rec_cols.table_keys_size() != key_size) {
        // TODO multi-key support
        return status::err_mismatch_key;
    }
    return status::ok;
}

static status check_valid_values_size(record_columns &rec_cols) {
    auto col_size = rec_cols.table_values_size();
    auto rec_size = static_cast<std::size_t>(rec_cols.values().size());
    // TODO support default values (currently all columns' values are necessary)
    if (rec_size < col_size) {
        return status::err_incomplete_columns;
    }
    // duplicate and unknown columns were already checked at check_valid_reccols() at first
    return status::ok;
}

static status check_valid_primary_key(record_columns &rec_cols, std::size_t key_size) {
    if (auto s = check_valid_key_size(rec_cols, key_size); s != status::ok) {
        return s;
    }
    return check_valid_columns(rec_cols.primary_keys());
}

status check_valid_primary_key(record_columns &rec_cols) {
    if (auto s = check_valid_reccols(rec_cols); s != status::ok) {
        return s;
    }
    return check_valid_primary_key(rec_cols, rec_cols.primary_keys().size());
}

static status check_valid_values(record_columns &rec_cols) {
    if (auto s = check_valid_values_size(rec_cols); s != status::ok) {
        return s;
    }
    return check_valid_columns(rec_cols.values());
}

status check_put_record(record_columns &rec_cols) {
    if (auto s = check_valid_reccols(rec_cols); s != status::ok) {
        return s;
    }
    if (auto s = check_valid_primary_key(rec_cols, rec_cols.primary_keys().size());
        s != status::ok) {
        return s;
    }
    return check_valid_values(rec_cols);
}

void add_key_column(std::string_view col_name,
                       tateyama::proto::kvs::data::Value const*value,
                       tateyama::proto::kvs::data::Record &record) {
    record.add_names(col_name.data());
    auto new_value = record.add_values();
    new_value->CopyFrom(*value);
}

status add_value_column(yugawara::storage::column const &column,
                         jogasaki::kvs::readable_stream &stream,
                         tateyama::proto::kvs::data::Record &record) {
    record.add_names(column.simple_name().data());
    auto new_value = record.add_values();
    if (auto s = deserialize(spec_value, column, stream, new_value);
            s != status::ok) {
        return s;
    }
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
