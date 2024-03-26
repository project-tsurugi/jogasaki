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

#include <memory>
#include <string>
#include <string_view>

#include <takatori/util/fail.h>
#include <yugawara/storage/configurable_provider.h>
#include <yugawara/storage/index.h>

#include <jogasaki/constants.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/executor/sequence/metadata_store.h>
#include <jogasaki/logging.h>
#include <jogasaki/plan/storage_processor.h>
#include <jogasaki/proto/metadata/storage.pb.h>
#include <jogasaki/utils/storage_metadata_serializer.h>

namespace jogasaki::recovery {

/**
 * @brief create storage option by serializing an index
 * @param idx the index to serialize
 * @param out [out] the output serialized string
 * @param option the serialization option
 * @return an error_info if the creation fails
 * Known error codes:
 *   error_code::unsupported_runtime_feature_exception if the default value data type is not supported
 * @return nullptr if successful
 */
std::shared_ptr<error::error_info> create_storage_option(
    yugawara::storage::index const& idx,
    std::string& out,
    utils::metadata_serializer_option const& option
);

/**
 * @brief deserialize storage option into a configurable provider
 * @param payload the storage option string
 * @param src the source configurable provider that the deserialized index definition depends on
 * @param out [out] the target configurable provider to extract the storage option into
 * @param overwrite whether to overwrite the objects in the target configurable provider
 * @return an error_info if the creation fails
 * Known error codes:
 *   error_code::target_already_exists_exception, if the deserialized object already exists (and `overwrite` is false)
 * @return nullptr if successful
 */
std::shared_ptr<error::error_info> deserialize_storage_option_into_provider(
    std::string_view payload,
    yugawara::storage::configurable_provider const &src,
    yugawara::storage::configurable_provider& out,
    bool overwrite
);

/**
 * @brief validate and extract the storage option
 * @param payload the storage option string
 * @param out [out] the output index definition
 * @return an error_info if the validation or extraction fails
 * @return nullptr if successful
 */
std::shared_ptr<error::error_info>
validate_extract(std::string_view payload, proto::metadata::storage::IndexDefinition& out);

/**
 * @brief merge the deserialized storage option into target configurable provider and remove from source
 * @param src the source configurable provider
 * @param target the target configurable provider
 * @param overwrite whether to overwrite the objects in the target configurable provider
 * @return an error_info if the operation fails
 * Known error codes:
 *   error_code::target_already_exists_exception, if the deserialized object already exists (and `overwrite` is false)
 * In case of error, `src` and `target` will be an undefined state. Avoid using them after the error.
 * @return nullptr if successful
 * @note intended to use in pair with deserialize_storage_option_into_provider to merge the deserialized storage option
 * after successful processing of the storage option
 * @note currently only supports merging a single index in `src`, multiple sequences, and tables underlying the index
 */
std::shared_ptr<error::error_info> merge_deserialized_storage_option(
    yugawara::storage::configurable_provider& src,
    yugawara::storage::configurable_provider& target,
    bool overwrite
);

}
