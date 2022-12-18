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
#pragma once

#include <initializer_list>

#include <yugawara/storage/table.h>
#include <yugawara/storage/index.h>
#include <yugawara/storage/configurable_provider.h>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/data/any.h>

#include <jogasaki/proto/metadata/storage.pb.h>

namespace jogasaki::utils {

/**
 * @brief serializer option
 */
struct metadata_serializer_option {
    bool synthesized_{};
};

/**
 * @brief serializer for storage metadata
 */
class storage_metadata_serializer {

public:
    /**
     * @brief create new object
     */
    storage_metadata_serializer() noexcept;

    /**
     * @brief destruct the object
     */
    ~storage_metadata_serializer() = default;

    storage_metadata_serializer(storage_metadata_serializer const& other) = default;
    storage_metadata_serializer& operator=(storage_metadata_serializer const& other) = default;
    storage_metadata_serializer(storage_metadata_serializer&& other) noexcept = default;
    storage_metadata_serializer& operator=(storage_metadata_serializer&& other) noexcept = default;

    /**
     * @brief serialize index as jogasaki::proto::metadata::storage::IndexDefinition
     * @param idx the primary index
     * @param out [out] to be filled with result serialized string
     * @details the index, base table and dependant sequences (if any) are serialized and stored in the output string
     * @return true when successful
     * @return false otherwise
     */
    bool serialize(yugawara::storage::index const& idx, std::string& out, metadata_serializer_option const& option = {});

    /**
     * @brief serialize index as jogasaki::proto::metadata::storage::IndexDefinition
     * @param idx the primary index
     * @param idef [out] to be filled with result serialized object
     * @details the index, base table and dependant sequences (if any) are serialized and stored in the output string
     * @return true when successful
     * @return false otherwise
     */
    bool serialize(yugawara::storage::index const& idx, proto::metadata::storage::IndexDefinition& idef, metadata_serializer_option const& option = {});

    /**
     * @brief deserialize protobuf msg for index
     * @param src the target string to deserialize as jogasaki::proto::metadata::storage::IndexDefinition
     * @param in configurable provider used to search base table definition (this is referenced when `out` doesn't
     * contain the base table definition.
     * @param out [out] to be filled with result objects (index, table, sequence, etc.)
     * @param overwrite whether overwrite existing entries in `out`
     * @return true when successful
     * @return false otherwise
     */
    bool deserialize(
        std::string_view src,
        yugawara::storage::configurable_provider const& in,
        yugawara::storage::configurable_provider& out,
        bool overwrite = false
    );

    /**
     * @brief deserialize protobuf msg for primary index
     * @param idef the target object to deserialize
     * @param in configurable provider used to search base table definition (this is referenced when `out` doesn't
     * contain the base table definition.
     * @param out [out] to be filled with result objects (index, table, sequence, etc.)
     * @param overwrite whether overwrite existing entries in `out`
     * @return true when successful
     * @return false otherwise
     */
    bool deserialize(
        proto::metadata::storage::IndexDefinition const& idef,
        yugawara::storage::configurable_provider const& in,
        yugawara::storage::configurable_provider& out,
        bool overwrite = false
    );
};

}

