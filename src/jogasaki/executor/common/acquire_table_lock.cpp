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
#include "acquire_table_lock.h"

#include <memory>
#include <optional>
#include <ostream>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/reference_extractor.h>
#include <takatori/util/reference_iterator.h>
#include <takatori/util/reference_list_view.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/column.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/sequence.h>
#include <yugawara/storage/table.h>

#include <jogasaki/constants.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/sequence/exception.h>
#include <jogasaki/executor/sequence/manager.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/handle_generic_error.h>
#include <jogasaki/utils/string_manipulation.h>

namespace jogasaki::executor::common {

using takatori::util::string_builder;

bool acquire_table_lock(request_context& context, std::string_view table_name, storage::storage_entry& out) {
    auto& smgr = *global::storage_manager();
    auto e = smgr.find_by_name(table_name);
    if(! e.has_value()) {
        set_error(
            context,
            error_code::target_not_found_exception,
            string_builder{} << "Table \"" << table_name << "\" not found." << string_builder::to_string,
            status::err_not_found
        );
        return false;
    }
    out = e.value();
    storage::storage_list stg{e.value()};
    auto& tx = *context.transaction();
    if (! tx.storage_lock()) {
        tx.storage_lock(smgr.create_unique_lock());
    }
    if(! smgr.add_locked_storages(stg, *tx.storage_lock())) {
        // table is locked by other operations
        set_error(
            context,
            error_code::sql_execution_exception,
            "DDL operation was blocked by other DML operation",
            status::err_illegal_operation
        );
        return false;
    }
    return true;
}

}
