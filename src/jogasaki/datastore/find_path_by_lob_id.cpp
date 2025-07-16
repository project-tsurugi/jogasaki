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
#include "find_path_by_lob_id.h"

#include <takatori/util/string_builder.h>
#include <limestone/api/limestone_exception.h>

#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/error/error_info_factory.h>

namespace jogasaki::datastore {

using takatori::util::string_builder;

status find_path_by_lob_id(
    lob::lob_id_type id,
    std::string& path,
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
        auto file = ds->get_blob_file(id);
        if (! file) {
            status res = status::err_invalid_state;
            error = create_error_info(error_code::lob_reference_invalid,
                string_builder{} << "failed to get the valid lob data for id:" << id << string_builder::to_string,
                res);
            return res;
        }
        path = file.path().string();
    } catch (limestone::api::limestone_blob_exception const& e) {
        status res = status::err_io_error;
        error = create_error_info(error_code::lob_file_io_error, e.what(), res);
        return res;
    }
    return status::ok;
}

}  // namespace jogasaki::datastore
