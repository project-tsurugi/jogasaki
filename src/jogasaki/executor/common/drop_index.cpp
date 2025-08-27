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
#include "drop_index.h"

#include <memory>
#include <ostream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/string_builder.h>
#include <yugawara/binding/extract.h>
#include <yugawara/storage/basic_configurable_provider.h>
#include <yugawara/storage/index.h>

#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/kvs/database.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>

#include "acquire_table_lock.h"
#include "validate_alter_table_auth.h"

namespace jogasaki::executor::common {

using takatori::util::string_builder;

drop_index::drop_index(takatori::statement::drop_index& ct) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind drop_index::kind() const noexcept {
    return model::statement_kind::drop_index;
}

bool drop_index::operator()(request_context& context) const {
    auto& provider = *context.storage_provider();
    auto i = yugawara::binding::extract_shared<yugawara::storage::index>(ct_->target());
    auto name = i->simple_name();
    if(! provider.find_index(name)) {
        set_error(
            context,
            error_code::target_not_found_exception,
            string_builder{} << "Target index \"" << name << "\" is not found." << string_builder::to_string,
            status::err_not_found
        );
        return false;
    }
    storage::storage_entry storage_id{};
    if(! acquire_table_lock(context, i->table().simple_name(), storage_id)) {
        return false;
    }
    if(! validate_alter_table_auth(context, storage_id)) {
        return false;
    }

    // try to delete stroage on kvs.

    auto stg = context.database()->get_storage(name);
    if (stg) {
        if(auto res = stg->delete_storage(); res != status::ok && res != status::not_found) {
            VLOG_LP(log_error) << res << "  " << name;
            set_error(
                context,
                error_code::sql_execution_exception,
                string_builder{} << "An error occurred in deleting storage. status:" << res << string_builder::to_string,
                status::err_unknown
            );
            return false;
        }
    } else {
        // kvs storage is already removed somehow, let's proceed and remove from metadata.
        VLOG_LP(log_info) << "kvs storage '" << name << "' not found.";
    }
    provider.remove_index(name);
    return true;
}
}
