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
#include "get_storage_by_index_name.h"

#include <memory>
#include <optional>
#include <string>
#include <string_view>

#include <jogasaki/executor/global.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/storage/storage_manager.h>

namespace jogasaki::utils {

std::unique_ptr<kvs::storage> get_storage_by_index_name(std::string_view index_name) {
    auto& smgr = *global::storage_manager();
    std::optional<std::string> storage_key = smgr.get_storage_key(index_name);
    if (! storage_key) {
        return nullptr;
    }
    return global::db()->get_storage(*storage_key);
}

}  // namespace jogasaki::utils
