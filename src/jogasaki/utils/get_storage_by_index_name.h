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

#include <memory>
#include <string_view>

#include <jogasaki/kvs/storage.h>

namespace jogasaki::utils {

/**
 * @brief get storage by index name using storage key from storage_manager
 * @param index_name the name of the index (table or secondary index)
 * @return storage object for the given index name
 * @return nullptr if the storage key is not found or storage does not exist
 */
std::unique_ptr<kvs::storage> get_storage_by_index_name(std::string_view index_name);

}  // namespace jogasaki::utils
