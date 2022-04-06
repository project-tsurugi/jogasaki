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
#include "drop_table.h"

#include <thread>
#include <chrono>

#include <yugawara/binding/extract.h>

#include <jogasaki/logging.h>

namespace jogasaki::executor::common {

drop_table::drop_table(takatori::statement::drop_table& ct) noexcept:
    ct_(std::addressof(ct))
{}

model::statement_kind drop_table::kind() const noexcept {
    return model::statement_kind::drop_table;
}

bool drop_table::operator()(request_context& context) const {
    BOOST_ASSERT(context.storage_provider());  //NOLINT
    // note: To fully clean up garbage, try to proceed further even if some entry removal failed or warned.
    auto& provider = *context.storage_provider();
    auto& c = yugawara::binding::extract<yugawara::storage::table>(ct_->target());
    if(auto res = provider.remove_index(c.simple_name());! res) {
        VLOG(log_error) << "primary index for table " << c.simple_name() << " not found";
        context.status_code(status::err_not_found);
    }
    if(auto res = provider.remove_relation(c.simple_name());! res) {
        VLOG(log_error) << "table " << c.simple_name() << " not found";
        context.status_code(status::err_not_found);
    }
    if(auto stg = context.database()->get_storage(c.simple_name())) {
        if(auto res = stg->delete_storage(); res != status::ok) {
            VLOG(log_error) << "deleting storage failed: " << res;
        }
    }

    // The delete_storage function removes entry from kvs system storage, but
    // shirakami delete_record requires some duration to become stable.
    // For convenience of DDL in testing, simply sleep here.
    // TODO remove when shirakami reflects the deletion in time
    std::this_thread::sleep_for(std::chrono::milliseconds{80});
    return true;
}
}