/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "describe_table_utils.h"

#include <jogasaki/executor/dto/common_column_utils.h>

namespace jogasaki::executor::dto {

namespace proto = jogasaki::proto;

describe_table from_proto(proto::sql::response::DescribeTable::Success const& src) {
    describe_table out{};
    if (! src.database_name().empty()) {
        out.database_name_ = src.database_name();
    }
    if (! src.schema_name().empty()) {
        out.schema_name_ = src.schema_name();
    }
    if (! src.table_name().empty()) {
        out.table_name_ = src.table_name();
    }
    for (auto const& c : src.columns()) {
        out.columns_.emplace_back(from_proto(c));
    }
    for (auto const& pk : src.primary_key()) {
        out.primary_key_.emplace_back(pk);
    }
    if (src.has_description()) {
        out.description_ = src.description();
    }
    return out;
}

proto::sql::response::DescribeTable::Success to_proto(describe_table const& src) {
    proto::sql::response::DescribeTable::Success out{};
    if (! src.database_name_.empty()) {
        out.set_database_name(src.database_name_);
    }
    if (! src.schema_name_.empty()) {
        out.set_schema_name(src.schema_name_);
    }
    if (! src.table_name_.empty()) {
        out.set_table_name(src.table_name_);
    }
    for (auto const& c : src.columns_) {
        *out.add_columns() = to_proto(c);
    }
    for (auto const& pk : src.primary_key_) {
        out.add_primary_key(pk);
    }
    if (src.description_) {
        out.set_description(*src.description_);
    }
    return out;
}

} // namespace jogasaki::executor
