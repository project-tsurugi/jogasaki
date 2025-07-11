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

#include <string_view>

#include <jogasaki/request_context.h>
#include <jogasaki/storage/storage_list.h>

namespace jogasaki::executor::common {

/**
 * @brief acquire table lock for the given table
 * @param context request context
 * @param table_name name of the table to acquire lock
 * @param out [out] storage entry to be filled with the table storage entry - valid only if the return value is true
 * @return true if the lock is acquired, false if the lock is already held by other transaction
 */
bool acquire_table_lock(request_context& context, std::string_view table_name, storage::storage_entry& out);

}
