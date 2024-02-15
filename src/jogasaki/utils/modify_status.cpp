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
#include "modify_status.h"

#include <jogasaki/utils/abort_transaction.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/configuration.h>

namespace jogasaki::utils {

bool modify_concurrent_operation_status(
    kvs::transaction& tx,
    status& res,
    bool scan
) {
    if(res == status::ok || res == status::not_found) {
        return false;
    }
    if(res != status::concurrent_operation) {
        return true;
    }
    auto treat_as_not_found = scan ? global::config_pool()->scan_concurrent_operation_as_not_found()
                                   : global::config_pool()->point_read_concurrent_operation_as_not_found();
    if(treat_as_not_found) {
        res = status::not_found;
        return false;
    }
    utils::abort_transaction(tx);
    res = status::err_serialization_failure;
    return true;
}

bool modify_concurrent_operation_status(
    transaction_context& tx,
    status& res,
    bool scan
) {
    return modify_concurrent_operation_status(*tx.object(), res, scan);
}

}  // namespace jogasaki::utils
