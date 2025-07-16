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

#include <string_view>
#include <jogasaki/constants.h>

namespace jogasaki::utils {

/**
 * @brief map schema name to storage namespace
 * @details for schema handling, we have two concepts - schema and namespace for storage in order to keep
 * backward compatibility. We use schema when facing sql compiler, but we use storage namespace when accessing kvs.
 * Historically jogasaki had storage namespace only, but we introduced schema when moving to new sql compiler.
 * So we need to map schema name to storage namespace and vice versa.
 */
[[nodiscard]] constexpr std::string_view map_schema_name_to_storage_namespace(std::string_view src) noexcept {
    if(src == public_schema_name) {
        return storage_namespace_for_public_schema;
    }
    return src;
}

}  // namespace jogasaki::utils
