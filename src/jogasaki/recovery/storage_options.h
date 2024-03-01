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

std::shared_ptr<error::error_info> create_storage_option(
    yugawara::storage::index const& i,
    std::string& storage,
    utils::metadata_serializer_option const& option
);

std::shared_ptr<error::error_info> deserialize_storage_option_into_provider(
    std::string_view payload,
    yugawara::storage::configurable_provider const &src,
    yugawara::storage::configurable_provider& target,
    bool overwrite
);

std::shared_ptr<error::error_info> validate_extract(std::string_view payload, proto::metadata::storage::IndexDefinition& out);

std::shared_ptr<error::error_info> merge_deserialized_storage_option(
    yugawara::storage::configurable_provider& src,
    yugawara::storage::configurable_provider& target,
    bool overwrite
);

}
