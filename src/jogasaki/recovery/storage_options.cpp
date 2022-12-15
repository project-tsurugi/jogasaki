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
#include "storage_options.h"

#include <yugawara/storage/configurable_provider.h>
#include <takatori/util/fail.h>

#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/logging.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

#include <jogasaki/proto/metadata/storage.pb.h>

#include "index.h"

namespace jogasaki::recovery {

bool serialize_deserialize_add_primary(
    yugawara::storage::index const& i,
    yugawara::storage::configurable_provider const &src,
    yugawara::storage::configurable_provider& provider,
    proto::metadata::storage::IndexDefinition& idef
) {
    idef = {};
    utils::storage_metadata_serializer ser{};
    if(! ser.serialize(i, idef)) {
        VLOG(log_error) << "serialization error";
        return false;
    }
    return recovery::deserialize_into_provider(idef, src, provider);
}

bool create_storage_option(
    yugawara::storage::index const&i,
    yugawara::storage::configurable_provider const &src,
    yugawara::storage::configurable_provider& target,

    std::string &storage) {
    storage.clear();
    proto::metadata::storage::IndexDefinition idef{};
    if(! serialize_deserialize_add_primary(i, src, target, idef)) {
        return false;
    }
    proto::metadata::storage::Storage stg{};
    stg.set_message_version(metadata_format_version);
    stg.set_allocated_index(&idef);

    std::stringstream ss{};
    if (!stg.SerializeToOstream(&ss)) {
        return false;
    }
    storage = ss.str();
    stg.release_index();
    return true;
}

}
