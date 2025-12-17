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
#include "datastore_prod.h"

namespace jogasaki::datastore {

datastore_prod::datastore_prod(limestone::api::datastore *ds) : ds_(ds) {}

std::unique_ptr<limestone::api::blob_pool> datastore_prod::acquire_blob_pool() {
    return ds_->acquire_blob_pool();
}

limestone::api::blob_file datastore_prod::get_blob_file(limestone::api::blob_id_type reference) {
    return ds_->get_blob_file(reference);
}

limestone::api::blob_reference_tag_type datastore_prod::generate_reference_tag(
    limestone::api::blob_id_type blob_id,
    std::uint64_t transaction_id) {
    return ds_->generate_reference_tag(blob_id, transaction_id);
}

}  // namespace jogasaki::datastore
