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

#include "datastore.h"

#include <jogasaki/kvs/database.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::datastore {

/**
 * @brief register lob file and publish new id
 * @param path the path for the lob file
 * @param db kvs database (nullptr is possible if no production datastore is
 * used)
 * @param tx transaction to keep the scope object (blob pool) for the lob data
 * @param out blob id assigned for the input lob data
 * @return status::ok when successful
 * @return any other error otherwise
 */
status register_lob(std::string_view path, kvs::database* db, transaction_context* tx, limestone::api::blob_id_type& out);

}  // namespace jogasaki::datastore
