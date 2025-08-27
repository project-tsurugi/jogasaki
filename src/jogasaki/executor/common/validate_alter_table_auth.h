/*
 * Copyright 2018-2024 Project Tsurugi.
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

#include <jogasaki/storage/storage_list.h>
#include <jogasaki/request_context.h>

namespace jogasaki::executor::common {

/**
 * @brief common function to validate authorization to alter table (i.e. admin or owns control on target table)
 * @param context the request context
 * @param storage_id the target storage id
 * @return true if the user is authorized to execute DDL on the target storage
 * @return false otherwise (then error info. is set on `context`)
 */
bool validate_alter_table_auth(
    request_context& context,
    storage::storage_entry storage_id
);

}