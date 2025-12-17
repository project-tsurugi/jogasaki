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
#include "assign_reference_tag.h"

#include <jogasaki/datastore/get_datastore.h>
#include <jogasaki/lob/lob_id.h>

namespace jogasaki::utils {

std::optional<lob::lob_reference_tag_type> assign_reference_tag(
    std::uint64_t transaction_id,
    lob::lob_id_type blob_id
) {
    auto* ds = datastore::get_datastore();
    if (ds == nullptr) {
        return std::nullopt;
    }
    try {
        return ds->generate_reference_tag(blob_id, transaction_id);
    } catch (...) {
        return std::nullopt;
    }
}

}  // namespace jogasaki::utils
