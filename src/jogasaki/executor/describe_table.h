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
#pragma once

#include <optional>
#include <string>
#include <vector>

#include <jogasaki/executor/common_column.h>

namespace jogasaki::executor {

/**
 * @brief lightweight representation of protocol buffer DescribeTable.Success
 */
struct describe_table {
    std::string database_name_{}; // currently unused, always empty
    std::string schema_name_{}; // currently unused, always empty
    std::string table_name_{};
    std::vector<common_column> columns_{};
    std::vector<std::string> primary_key_{};
    std::optional<std::string> description_{};

    /**
     * @brief equality comparison.
     */
    friend bool operator==(describe_table const& a, describe_table const& b) {
        return a.database_name_ == b.database_name_
            && a.schema_name_ == b.schema_name_
            && a.table_name_ == b.table_name_
            && a.columns_ == b.columns_
            && a.primary_key_ == b.primary_key_
            && a.description_ == b.description_;
    }

    /**
     * @brief inequality comparison.
     */
    friend bool operator!=(describe_table const& a, describe_table const& b) {
        return !(a == b);
    }

    /**
     * @brief stream operator for describe_table
     */
    friend inline std::ostream& operator<<(std::ostream& out, describe_table const& v) {
        out << "describe_table{name:\"" << v.table_name_ << "\"";
        if (v.description_) {
            out << " desc:\"" << *v.description_ << "\"";
        }
        out << " columns:[";
        for (size_t i = 0; i < v.columns_.size(); ++i) {
            if (i != 0) out << ',';
            out << v.columns_[i];
        }
        out << "] pk:[";
        for (size_t i = 0; i < v.primary_key_.size(); ++i) {
            if (i != 0) out << ',';
            out << "\"" << v.primary_key_[i] << "\"";
        }
        out << "]}";
        return out;
    }
};

} // namespace jogasaki::executor
