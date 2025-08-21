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
#include "register_lob.h"

#include <limestone/api/limestone_exception.h>

#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/error/error_info_factory.h>

namespace jogasaki::datastore {

static status register_lob_impl(
    std::optional<std::string_view> path,
    std::optional<std::string_view> data,
    lob::lob_id_type in,
    bool is_temporary,
    transaction_context* tx,
    lob::lob_id_type& out,
    std::shared_ptr<error::error_info>& error
) {
    auto* ds = get_datastore();
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
        if (data.has_value()) {
            out = tx->blob_pool()->register_data(data.value());
        } else if (path.has_value()) {
            out = tx->blob_pool()->register_file(boost::filesystem::path{std::string{path.value()}}, is_temporary);
        } else {
            out = tx->blob_pool()->duplicate_data(in);
        }
    } catch (limestone::api::limestone_blob_exception const& e) {
        // assuming only the possible scenario is IO error with register_file
        status res = status::err_io_error;
        error = create_error_info(error_code::lob_file_io_error, e.what(), res);
        return res;
    }
    return status::ok;
}

status register_lob(
    std::string_view path,
    bool is_temporary,
    transaction_context* tx,
    lob::lob_id_type& out,
    std::shared_ptr<error::error_info>& error
) {
    return register_lob_impl(path, {}, {}, is_temporary, tx, out, error);
}

status register_lob_data(
    std::string_view data,
    transaction_context* tx,
    lob::lob_id_type& out,
    std::shared_ptr<error::error_info>& error
) {
    return register_lob_impl({}, data, {}, {}, tx, out, error);
}

status duplicate_lob(
    lob::lob_id_type in,
    transaction_context* tx,
    lob::lob_id_type& out,
    std::shared_ptr<error::error_info>& error
) {
    return register_lob_impl({}, {}, in, {}, tx, out, error);
}

}  // namespace jogasaki::datastore
