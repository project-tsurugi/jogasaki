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
#include "system_storage.h"

#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <glog/logging.h>

#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>
#include <sharksfin/StorageOptions.h>

#include <jogasaki/constants.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/tables.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/recovery/storage_options.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

namespace jogasaki::kvs {

status create_storage_from_provider(std::string_view storage_key, std::string_view index_name, yugawara::storage::configurable_provider const& provider) {
    auto stg = global::db()->get_storage(storage_key);
    if(stg) {
        return status::ok;
    }
    auto i = provider.find_index(index_name);
    if(! i) {
        return status::err_unknown;
    }
    std::string storage{};
    if(auto err = recovery::create_storage_option(*i, storage, utils::metadata_serializer_option{true})) {
        if(! VLOG_IS_ON(log_trace)) {  // avoid duplicate log entry with log_trace
            VLOG_LP(log_error) << "error_info:" << *err;
        }
        return status::err_unknown;
    }
    ::sharksfin::StorageOptions options{};
    options.payload(std::move(storage));
    if(stg = global::db()->create_storage(index_name, options); ! stg) {
        return status::err_unknown;
    }
    return status::ok;
}

status setup_system_storage() {
    // if system table doesn't exist, create a kvs store, that will be recovered later in this start-up process
    auto provider = std::make_shared<yugawara::storage::configurable_provider>(); // just for serialize
    executor::add_builtin_tables(*provider);
    return create_storage_from_provider(system_sequences_name, system_sequences_name, *provider);
}

}
