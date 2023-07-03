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
#include "record_util.h"
#include "mapped_record.h"

using takatori::util::throw_exception;

namespace jogasaki::api::kvsservice {

record_util::record_util(jogasaki::api::database* db) :
    db_(dynamic_cast<jogasaki::api::impl::database *>(db)) {
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

status record_util::check_put_record(std::string_view table_name, tateyama::proto::kvs::data::Record const &record) {
    auto table = db_->tables()->find_table(table_name);
    if (table == nullptr) {
        return status::err_unknown; // FIXME
    }
    if (record.names_size() != record.values_size()) {
        return status::err_invalid_argument;
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

}
