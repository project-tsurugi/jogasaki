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

#include <jogasaki/datastore/get_datastore.h>

namespace jogasaki::datastore {

status register_lob(std::string_view path, kvs::database* db, transaction_context* tx, limestone::api::blob_id_type& out) {
    auto* ds = get_datastore(db);
    if (tx) {
        if (! tx->blob_pool()) {
            // TODO exception / error handling with limestone
            tx->blob_pool(ds->acquire_blob_pool());
        }
    } else {
        // for testing
        auto pool = ds->acquire_blob_pool();
        out = pool->register_file(boost::filesystem::path{std::string{path}}, false);
        return status::ok;
    }
    out = tx->blob_pool()->register_file(boost::filesystem::path{std::string{path}}, false);
    return status::ok;
}

}  // namespace jogasaki::datastore
