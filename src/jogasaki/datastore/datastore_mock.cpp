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
#include "datastore_mock.h"

#include <jogasaki/datastore/blob_pool_mock.h>

namespace jogasaki::datastore {

std::unique_ptr<limestone::api::blob_pool> datastore_mock::acquire_blob_pool() {
    return std::make_unique<blob_pool_mock>(this);
}

limestone::api::blob_file datastore_mock::get_blob_file(limestone::api::blob_id_type reference) {
    if (id_to_path_.count(reference) == 0) {
        return limestone::api::blob_file{{}, false};
    }
    return limestone::api::blob_file{id_to_path_.at(reference), true};
}

limestone::api::blob_reference_tag_type datastore_mock::generate_reference_tag(
    limestone::api::blob_id_type blob_id,
    std::uint64_t transaction_id) {
    return blob_id ^ transaction_id;
}

} // namespace jogasaki::datastore
