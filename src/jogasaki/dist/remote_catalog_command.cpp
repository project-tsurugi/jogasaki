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
#include "remote_catalog_command.h"

namespace jogasaki::dist {

status execute_create_foreign_catalog(
    create_foreign_catalog_command const& command,
    remote_catalog_manager& catalogs,
    remote_server_channel_manager const& channels,
    std::string& diagnostic
) {
    diagnostic.clear();
    auto const& reference = command.reference;
    if (!catalogs.entries()) {
        diagnostic = "remote catalog manager is not available";
        return status::err_invalid_state;
    }
    if (reference.remote_server_name == "local") {
        diagnostic = "remote server name 'local' is reserved";
        return status::err_invalid_argument;
    }
    if (!valid_remote_catalog_identifier(reference.catalog_name) ||
        !valid_remote_catalog_identifier(reference.remote_server_name)) {
        diagnostic = "invalid remote catalog or server name";
        return status::err_invalid_argument;
    }
    if (!channels.find_channel(reference.remote_server_name)) {
        if (command.if_server_exists) {
            return status::ok;
        }
        diagnostic = "remote server not found: " + reference.remote_server_name;
        return status::err_not_found;
    }
    if (catalogs.find_catalog(reference.catalog_name)) {
        if (command.if_catalog_not_exists) {
            return status::ok;
        }
        diagnostic = "remote catalog already exists: " + reference.catalog_name;
        return status::err_already_exists;
    }
    auto st = catalogs.create(reference, channels, diagnostic);
    if (st == status::err_invalid_argument) {
        // A reservation is not a published catalog: do not report a conditional success
        // while another fetch could still fail.
        if (!catalogs.find_catalog(reference.catalog_name)) {
            diagnostic = "remote catalog creation is in progress: " + reference.catalog_name;
            return status::err_serialization_failure;
        }
        if (command.if_catalog_not_exists) {
            diagnostic.clear();
            return status::ok;
        }
        return status::err_already_exists;
    }
    return st;
}

status execute_drop_foreign_catalog(
    drop_foreign_catalog_command const& command,
    remote_catalog_manager& catalogs,
    std::string& diagnostic
) {
    diagnostic.clear();
    if (!catalogs.entries()) {
        diagnostic = "remote catalog manager is not available";
        return status::err_invalid_state;
    }
    auto st = catalogs.drop(command.catalog_name);
    if (st == status::err_invalid_argument) {
        if (command.if_catalog_exists) {
            return status::ok;
        }
        diagnostic = "remote catalog not found: " + command.catalog_name;
        return status::err_not_found;
    }
    if (st != status::ok) {
        diagnostic = "remote catalog manager is not available";
    }
    return st;
}

}  // namespace jogasaki::dist
