/*
 * Copyright 2018-2020 tsurugi project.
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

#include <yugawara/binding/extract.h>

#include <jogasaki/logging.h>
#include <jogasaki/utils/string_manipulation.h>

namespace jogasaki::executor::common {

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
        context.status_code(status::err_not_found);
        return false;
    }
    // try to delete stroage on kvs.

    auto stg = context.database()->get_storage(name);
    if (stg) {
        if(auto res = stg->delete_storage(); res != status::ok && res != status::not_found) {
            VLOG(log_error) << res << " error on deleting storage " << name;
            context.status_code(status::err_unknown);
            return false;
        }
    } else {
        // kvs storage is already removed somehow, let's proceed and remove from metadata.
        VLOG(log_info) << "kvs storage '" << name << "' not found.";
    }
    provider.remove_index(name);
    return true;
}
}
