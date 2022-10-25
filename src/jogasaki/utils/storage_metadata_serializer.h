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

namespace jogasaki::utils {

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
    ~storage_metadata_serializer();

    storage_metadata_serializer(storage_metadata_serializer const& other) = default;
    storage_metadata_serializer& operator=(storage_metadata_serializer const& other) = default;
    storage_metadata_serializer(storage_metadata_serializer&& other) noexcept = default;
    storage_metadata_serializer& operator=(storage_metadata_serializer&& other) noexcept = default;

    /**
     * @brief serialize primary index as jogasaki::proto::metadata::storage::IndexDefinition
     * @param idx the primary index
     * @details the primary index, its base table and dependant sequences (if any) are serialized and contained in the output string
     * @return true when successful
     * @return false otherwise
     */
    bool serialize_primary_index(yugawara::storage::index const& idx, std::string& out);

    /**
     * @brief serialize secondary index
     * @param idx the secondary index
     * @details the secondary index definition is serialized
     * @return true when successful
     * @return false otherwise
     */
    bool serialize_secondary_index(yugawara::storage::index const& idx, std::string& out) {
        (void) idx;
        (void) out;
        return true;
    }

    /**
     * @brief deserialize protobuf msg for primary index
     * @param src the target string to deserialize as jogasaki::proto::metadata::storage::IndexDefinition
     * @param provider [out] to be filled with result objects (index, table, sequence, etc.)
     * @return true when successful
     * @return false otherwise
     */
    bool deserialize(
        std::string_view src,
        yugawara::storage::configurable_provider const& in,
        std::shared_ptr<yugawara::storage::configurable_provider>& out
    );

//    std::shared_ptr<yugawara::storage::configurable_provider> const& provider() {
//        return provider_;
//    }
//private:
//    std::shared_ptr<yugawara::storage::configurable_provider> provider_{};
};

}

