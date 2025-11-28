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

#include <string_view>

#include <jogasaki/status.h>

#include <yugawara/storage/configurable_provider.h>

namespace jogasaki::kvs {

/**
 * @brief setup system storage
 * @details create the system built-in storage if it does not exist
 * @return status::ok if successful
 * @return status::err_unknown if error
 */
status setup_system_storage();

/**
 * @brief create storage with the index definition in the provider
 * @details create the built-in storage if it does not exist. If `storage_key` and `index_name` are identical,
 * this function create pre-1.8 storage, that is, `storage_key` is used as identifier for sharksfin,
 * but the storage metadata does not contain `IndexDefinition.storage_key` field.
 * @param storage_key the storage key assigned for the storage
 * @param index_name the index name used to find the index definition in the provider
 * @param provider provides the indices and other related objects
 * @return status::ok if successful
 * @return status::err_unknown if error
 * @note this function is intended for backward compatibility to support built-in-like tables.
 * Once you don't need built-in tables for testing/benchmarking, this function can be removed or
 * simplified for specific use by setup_system_storage above.
 */
status create_storage_from_provider(
    std::string_view storage_key,
    std::string_view index_name,
    yugawara::storage::configurable_provider const& provider
);

}
