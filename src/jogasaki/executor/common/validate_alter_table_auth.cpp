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
#include "validate_alter_table_auth.h"

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/storage/storage_manager.h>
#include <jogasaki/error/error_info_factory.h>

#include <jogasaki/request_context.h>

namespace jogasaki::executor::common {

bool validate_alter_table_auth(
    request_context& context,
    storage::storage_entry storage_id
) {
    auto& smgr = *global::storage_manager();
    auto stg = smgr.find_entry(storage_id);
    if (auto& s = context.req_info().request_source()) {
        if(s->session_info().user_type() != tateyama::api::server::user_type::administrator) {
            auto username = s->session_info().username();
            if(! username.has_value() ||
               ! stg->allows_user_actions(username.value(), auth::action_set{auth::action_kind::control})) {
                VLOG_LP(log_error) << "insufficient authorization user:\""
                                   << (username.has_value() ? username.value() : "")
                                   << "\"";
                set_error_context(
                    context,
                    error_code::permission_error,
                    "insufficient authorization for the requested operation",
                    status::err_illegal_operation
                );
                return false;
            }
        }
    }
    return true;
}

}
