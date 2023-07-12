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

status get_table(jogasaki::api::impl::database* db_,
                        std::string_view table_name,
                        std::shared_ptr<yugawara::storage::table const> &table);

status check_put_record(std::shared_ptr<yugawara::storage::table const> &table,
                               tateyama::proto::kvs::data::Record const &record,
                               std::vector<tateyama::proto::kvs::data::Value const*> &key_values,
                               std::vector<tateyama::proto::kvs::data::Value const*> &value_values);

status check_primary_key(std::shared_ptr<yugawara::storage::table const> &table,
                                tateyama::proto::kvs::data::Record const &primary_key,
                                std::vector<tateyama::proto::kvs::data::Value const*> &key_values);

status make_record(std::shared_ptr<yugawara::storage::table const> &table,
                          tateyama::proto::kvs::data::Record const &primary_key,
                          sharksfin::Slice const &value_slice,
                          tateyama::proto::kvs::data::Record &record);
}
