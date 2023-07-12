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
#pragma once

#include <takatori/util/exception.h>
#include <jogasaki/api/impl/database.h>

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
 * @brief check the record is valid for put, and divide the values between primary key(s) and others
 * @param table the schema of the table
 * @param record the record data to be put
 * @param key_values [out]the value(s) of primary key(s)
 * @param value_values [out]the values of non primary key
 * @return status::ok if succeeded
 * @return status::err_invalid_argument if length of names and values of the record is invalid
 * @return status::err_incomplete_columns if the record doesn't have enough columns for the primary key
 * @return status::err_invalid_argument if the record has too many columns for the specified table
 * @return status::err_column_not_found if the record has invalid name of the column
 * @return status::err_column_type_mismatch if the column data type is different from the table schema
 * @return otherwise if error was occurred
 */
status check_put_record(std::shared_ptr<yugawara::storage::table const> &table,
                       tateyama::proto::kvs::data::Record const &record,
                       std::vector<tateyama::proto::kvs::data::Value const*> &key_values,
                       std::vector<tateyama::proto::kvs::data::Value const*> &value_values);

/**
 * @brief check the key is valid as primary key(s) of the table
 * @param table the schema of the table
 * @param primary_key the value(s) of the primary key(s)
 * @param key_values [out]the values of primary key(s)
 * @return status::ok if succeeded
 * @return status::err_invalid_argument if length of names and values of the record is invalid
 * @return status::err_invalid_argument if the record has too many columns for the specified table
 * @return status::err_column_not_found if the record has invalid name of the column
 * @return status::err_column_type_mismatch if the column data type is different from the table schema
 * @return otherwise if error was occurred
 */
status check_primary_key(std::shared_ptr<yugawara::storage::table const> &table,
                        tateyama::proto::kvs::data::Record const &primary_key,
                        std::vector<tateyama::proto::kvs::data::Value const*> &key_values);

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
constexpr bool nullable_primary_key = false;
constexpr bool nullable_value = true;


}
