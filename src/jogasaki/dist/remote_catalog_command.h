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



#include <string>

#include <jogasaki/dist/remote_catalog_manager.h>
#include <jogasaki/dist/remote_server_channel_manager.h>
#include <jogasaki/status.h>

namespace jogasaki::dist {

/**
 * @brief engine-side input of CREATE FOREIGN CATALOG.
 * @details Stands in for takatori::statement::create_foreign_catalog, which the compiler does not provide yet.
 * The flags correspond to if_catalog_not_exists and if_server_exists of its feature set.
 */
struct create_foreign_catalog_command {
    remote_catalog_reference reference{};
    bool if_catalog_not_exists{};
    bool if_server_exists{};
};

/**
 * @brief engine-side input of DROP FOREIGN CATALOG.
 * @details Stands in for takatori::statement::drop_foreign_catalog. The flag corresponds to if_catalog_exists.
 */
struct drop_foreign_catalog_command {
    std::string catalog_name{};
    bool if_catalog_exists{};
};

/**
 * @brief executes CREATE FOREIGN CATALOG against the runtime registry.
 * @details Evaluates the conditional clauses, which the compiler leaves to the engine, then calls
 * remote_catalog_manager::create(). Checks the server before the published catalog.
 * When both clauses are given the catalog is created only if both conditions hold.
 * @return status::ok if created or skipped by a clause,
 * status::err_invalid_argument if a catalog or server name is invalid or reserved,
 * status::err_invalid_state if the manager is not available,
 * status::err_not_found if the server is not configured,
 * status::err_already_exists if the catalog exists,
 * status::err_serialization_failure if another creation of the same catalog is in progress,
 * otherwise the status of remote_catalog_manager::create()
 */
[[nodiscard]] status execute_create_foreign_catalog(
    create_foreign_catalog_command const& command,
    remote_catalog_manager& catalogs,
    remote_server_channel_manager const& channels,
    std::string& diagnostic);

/**
 * @brief executes DROP FOREIGN CATALOG against the runtime registry.
 * @return status::ok if dropped or skipped by the clause, status::err_not_found if the catalog does not exist,
 * status::err_invalid_state if the manager is not available,
 * otherwise the status of remote_catalog_manager::drop()
 */
[[nodiscard]] status execute_drop_foreign_catalog(
    drop_foreign_catalog_command const& command,
    remote_catalog_manager& catalogs,
    std::string& diagnostic);

}  // namespace jogasaki::dist
