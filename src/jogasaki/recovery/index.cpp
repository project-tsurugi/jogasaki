/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include "index.h"

#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/utils/storage_metadata_exception.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

namespace jogasaki::recovery {

std::shared_ptr<error::error_info> deserialize_into_provider(
    proto::metadata::storage::IndexDefinition const& idef,
    yugawara::storage::configurable_provider const& src,
    yugawara::storage::configurable_provider& out,
    bool overwrite
) {
    utils::storage_metadata_serializer ser{};
    try {
        ser.deserialize(idef, src, out, overwrite);
    } catch(utils::storage_metadata_exception const& e) {
        return create_error_from_exception(e);
    }
    return {};
}

std::shared_ptr<error::error_info> serialize_index(
    const yugawara::storage::index &idx,
    proto::metadata::storage::IndexDefinition &out,
    utils::metadata_serializer_option const& option
) {
    out = {};
    utils::storage_metadata_serializer ser{};
    try {
        ser.serialize(idx, out, option);
    } catch (utils::storage_metadata_exception const& e) {
        return create_error_from_exception(e);
    }
    return {};
}

}
