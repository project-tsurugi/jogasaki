/*
 * Copyright 2018-2026 Project Tsurugi.
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

#include <atomic>
#include <string>
#include <vector>

namespace jogasaki::storage {

/**
 * @brief run the storage maintenance cycle once synchronously
 * @details finds all delete-reserved storages, deletes those whose ref_transaction_count is 0,
 * and removes them from the storage manager. intended to be called from the background
 * maintenance thread, but can also be invoked directly from tests.
 * @param stop_requested optional pointer to an atomic flag; if provided and the flag is true
 * before a delete_storage call, the function returns early without processing remaining entries.
 * Pass nullptr (the default) when early termination is not needed.
 * @return a list of storage names that were deleted in this invocation
 */
std::vector<std::string> maintenance_storage(std::atomic_bool const* stop_requested = nullptr);

} // namespace jogasaki::storage
