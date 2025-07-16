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
#pragma once

#include <string_view>
#include <vector>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/request_context.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/interference_size.h>

namespace jogasaki::executor::process::impl::ops {

namespace details {

/**
 * @brief secondary index field info
 * @details mapper uses these fields to extract the primary key from secondary key
 */
struct cache_align secondary_index_field_info {

    /**
     * @brief create new field information
     * @param type type of the field
     * @param source_nullable whether the target field is nullable or not
     * @param spec the spec of the target field used for encode/decode
     */
    secondary_index_field_info(
        meta::field_type type,
        bool source_nullable,
        kvs::coding_spec spec
    );

    meta::field_type type_{}; //NOLINT
    bool source_nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
};

}  // namespace details

/**
 * @brief index fields mapper object
 * @details this object knows the mapping from secondary index (if any) to primary, receives the key/value and fill
 * relation fields by resolving the primary key and identifying field values on the primary index.
 */
class index_field_mapper {
public:
    using memory_resource = memory::lifo_paged_memory_resource;

    /**
     * @brief create empty object
     */
    index_field_mapper() = default;

    /**
     * @brief common constructor for both cases on whether secondary index is used or not
     * @param use_secondary whether secondary index is used
     * @param primary_key_fields info. on primary index key fields
     * @param primary_value_fields info. on primary index value fields
     * @param secondary_key_fields info. on secondary index key fields
     */
    index_field_mapper(
        bool use_secondary,
        std::vector<index::field_info> primary_key_fields,
        std::vector<index::field_info> primary_value_fields,
        std::vector<details::secondary_index_field_info> secondary_key_fields
    );

    /**
     * @brief create new object using secondary
     * @param primary_key_fields info. on primary index key fields
     * @param primary_value_fields info. on primary index value fields
     * @param secondary_key_fields info. on secondary index key fields
     */
    index_field_mapper(
        std::vector<index::field_info> primary_key_fields,
        std::vector<index::field_info> primary_value_fields,
        std::vector<details::secondary_index_field_info> secondary_key_fields
    );

    /**
     * @brief create new object without secondary
     * @param primary_key_fields info. on primary index key fields
     * @param primary_value_fields info. on primary index value fields
     */
    index_field_mapper(
        std::vector<index::field_info> primary_key_fields,
        std::vector<index::field_info> primary_value_fields
    );

    /**
     * @brief process input record, map key/value and fill the variables accessing the secondary index if necessary
     * @details this function identifies the primary index record and fills the variables. If error occurs,
     * `req_context` is filled with error_info and error status code is returned
     * @param key the key of the input record (either primary or secondary)
     * @param value the value of the input record (empty if secondary)
     * @param target the record reference to fill the variables
     * @param stg the primary index storage
     * @param tx the transaction context
     * @param resource the memory resource to allocate temporary buffers
     * @param req_context the request context to report errors (nullptr if reporting is not necessary)
     * @return status::ok if the operation is successful
     * @return error status code otherwise
     */
    [[nodiscard]] status process(
        std::string_view key,
        std::string_view value,
        accessor::record_ref target,
        kvs::storage& stg,
        kvs::transaction& tx,
        memory_resource* resource,
        request_context& req_context
    );

private:
    bool use_secondary_{};
    std::vector<index::field_info> primary_key_fields_{};
    std::vector<index::field_info> primary_value_fields_{};
    std::vector<details::secondary_index_field_info> secondary_key_fields_{};

    status consume_secondary_key_fields(
        std::vector<details::secondary_index_field_info> const& fields,
        kvs::readable_stream& stream
    );

    std::string_view extract_primary_key(
        std::string_view key
    );

    status find_primary_index(
        std::string_view key,
        kvs::storage& stg,
        kvs::transaction& tx,
        std::string_view& value_out,
        request_context& req_context
    );

    status populate_field_variables(
        std::string_view key,
        std::string_view value,
        accessor::record_ref target,
        memory_resource* resource
    );

};

}


