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
#include "index.h"

#include <yugawara/binding/extract.h>
#include <takatori/util/fail.h>

#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/constants.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

#include <jogasaki/proto/metadata/storage.pb.h>

namespace jogasaki::recovery {

bool deserialize_into_provider(
    proto::metadata::storage::IndexDefinition const& idef,
    yugawara::storage::configurable_provider const& src,
    yugawara::storage::configurable_provider& target,
    bool overwrite
) {
    utils::storage_metadata_serializer ser{};
    auto deserialized = std::make_shared<yugawara::storage::configurable_provider>();
    if(! ser.deserialize(idef, src, target, overwrite)) {
        VLOG_LP(log_error) << "deserialization error";
        return false;
    }
    return true;
}

bool serialize_index(
    const yugawara::storage::index &i,
    proto::metadata::storage::IndexDefinition &idef,
    utils::metadata_serializer_option const& option
) {
    idef = {};
    utils::storage_metadata_serializer ser{};
    if(! ser.serialize(i, idef, option)) {
        VLOG_LP(log_error) << "serialization error";
        return false;
    }
    return true;
}

}
