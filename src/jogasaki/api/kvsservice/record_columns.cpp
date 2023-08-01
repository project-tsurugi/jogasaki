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
#include "record_columns.h"
#include "mapped_record.h"

namespace jogasaki::api::kvsservice {

record_columns::record_columns(std::shared_ptr<yugawara::storage::table const> &table,
        tateyama::proto::kvs::data::Record const &record,
        bool only_keys) : table_(table) {
    mapped_record m_rec{record};
    const auto primary = table->owner()->find_primary_index(*table);
    for (const auto &key_col: primary->keys()) {
        const auto &column = key_col.column();
        // TODO should be case-insensitive
        const auto value = m_rec.get_value(column.simple_name());
        primary_keys_.emplace_back(&column, value);
    }
    if (only_keys) {
        return;
    }
    for (const auto &value_col: primary->values()) {
        const auto &column = value_col.get();
        // TODO should be case-insensitive
        const auto value = m_rec.get_value(column.simple_name());
        values_.emplace_back(&column, value);
    }
}

}
