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
#pragma once

#include <yugawara/storage/configurable_provider.h>
#include <takatori/util/fail.h>

#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/logging.h>
#include <jogasaki/constants.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

#include <jogasaki/proto/metadata/storage.pb.h>

namespace jogasaki::recovery {

/**
 * @brief deserialize the serialized index definition and extract into a configurable provider
 * @param idef the serialized index definition
 * @param src the source configurable provider that the index definition depends on
 * @param out [out] the target configurable provider to extract the index definition into
 * @param overwrite whether to overwrite the objects in the target configurable provider
 * @return an error_info if the deserialization fails
 * @return nullptr if successful
 */
std::shared_ptr<error::error_info> deserialize_into_provider(
    proto::metadata::storage::IndexDefinition const& idef,
    yugawara::storage::configurable_provider const& src,
    yugawara::storage::configurable_provider& out,
    bool overwrite = false
);

/**
 * @brief serialize an index into proto::metadata::storage::IndexDefinition
 * @param idx the index to serialize
 * @param out [out] the output serialized index definition
 * @param option the serialization option
 * @return an error_info if the serialization fails
 *   error_code::unsupported_runtime_feature_exception if the default value data type is not supported
 * @return nullptr if successful
*/
std::shared_ptr<error::error_info> serialize_index(
    yugawara::storage::index const& idx,
    proto::metadata::storage::IndexDefinition& out,
    utils::metadata_serializer_option const& option
);

}
