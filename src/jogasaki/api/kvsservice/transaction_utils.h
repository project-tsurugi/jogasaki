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
#pragma once

#include <memory>
#include <string_view>

#include <takatori/util/exception.h>
#include <yugawara/storage/table.h>
#include <tateyama/proto/kvs/data.pb.h>
#include <sharksfin/Slice.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/kvsservice/status.h>
#include <jogasaki/kvs/coder.h>

#include "record_columns.h"

namespace jogasaki::api::kvsservice {

/**
 * @brief get the schema of the table
 * @param db database
 * @param table_name full-qualified name of the table
 * @param table [out]the schema of the table
 * @return status::ok if succeeded
 * @return otherwise if error was occurred
 */
status get_table(jogasaki::api::impl::database* db,
                std::string_view table_name,
                std::shared_ptr<yugawara::storage::table const> &table);

/**
 * @brief check whether the table has secondary index
 * @param table the table
 * @return true if the table has secondary index, false otherwise
 */
bool has_secondary_index(std::shared_ptr<yugawara::storage::table const> &table);

/**
 * @brief check whether the record has valid data or not
 * @param record the record
 * @return true if valid, false otherwise
 */
bool is_valid_record(tateyama::proto::kvs::data::Record const &record) noexcept;

/**
 * @brief check whether the record is valid for put
 * @param rec_cols list of columns
 * @return status::ok if succeeded
 * @return status::err_incomplete_columns if the record doesn't have enough columns for the primary key
 * @return status::err_invalid_argument if the record has too many columns for the specified table
 * @return status::err_column_not_found if the record has invalid name of the column
 * @return status::err_column_type_mismatch if the column data type is different from the table schema
 * @return otherwise if error was occurred
 */
status check_put_record(record_columns &rec_cols);

/**
 * @brief check whether the key is valid as primary key(s) of the table
 * @param rec_cols list of columns as a primary key
 * @return status::ok if succeeded
 * @return status::err_incomplete_columns if the record doesn't have enough columns for the primary key
 * @return status::err_invalid_argument if the record has too many columns for the specified table
 * @return status::err_column_not_found if the record has invalid name of the column
 * @return status::err_column_type_mismatch if the column data type is different from the table schema
 * @return otherwise if error was occurred
 */
status check_valid_primary_key(record_columns &rec_cols);

/**
 * @brief make a record with the primary key and values
 * @param table the schema of the table
 * @param primary_key the value(s) of the primary key(s)
 * @param value_slice the read slice data of the values
 * @param record [out]the record with the primary key and values
 * @return status::ok if succeeded
 * @return status::err_invalid_argument if deserialize the value_slice failed
 * @return otherwise if error was occurred
 */
status make_record(std::shared_ptr<yugawara::storage::table const> &table,
                  tateyama::proto::kvs::data::Record const &primary_key,
                  sharksfin::Slice const &value_slice,
                  tateyama::proto::kvs::data::Record &record);

constexpr jogasaki::kvs::coding_spec spec_primary_key = jogasaki::kvs::spec_key_ascending;
constexpr jogasaki::kvs::coding_spec spec_value = jogasaki::kvs::spec_value;

}
