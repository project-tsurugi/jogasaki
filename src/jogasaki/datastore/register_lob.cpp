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
#include "register_lob.h"

#include <limestone/api/limestone_exception.h>

#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/error/error_info_factory.h>

namespace jogasaki::datastore {

status register_lob(
    std::string_view path,
    transaction_context* tx,
    lob::lob_id_type& out,
    std::shared_ptr<error::error_info>& error
) {
    auto* ds = get_datastore(tx->database());
    if (ds == nullptr) {
        // should not happen normally
        status res = status::err_invalid_state;
        error = create_error_info(error_code::sql_execution_exception, "failed to access datastore object", res);
        return res;
    }
    try {
        if (! tx->blob_pool()) {
            tx->blob_pool(ds->acquire_blob_pool());
        }
        out = tx->blob_pool()->register_file(boost::filesystem::path{std::string{path}}, false);
    } catch (limestone::api::limestone_blob_exception const& e) {
        status res = status::err_io_error;
        // TODO improve error code
        error = create_error_info(error_code::sql_execution_exception, e.what(), res);
        return res;
    }
    return status::ok;
}

}  // namespace jogasaki::datastore
