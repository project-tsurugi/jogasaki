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

#include <vector>

#include <takatori/relation/write.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <yugawara/storage/index.h>

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/coder.h>

#include "write_primary_context.h"

namespace jogasaki::index {

using takatori::util::maybe_shared_ptr;

/**
 * @brief undefined offset value
 */
static constexpr std::size_t npos = static_cast<std::size_t>(-1);

/**
 * @brief primary target for write
 * @details this object represents write operation interface for primary index
 * This hides encoding/decoding details under field mapping and provide write access api based on key/value record_ref.
 * This object has following record definitions and each is represented by field mapping.
 * - input key record
 *   - the source columns to generate key for finding target entry in the primary index
 * - extracted key/value records
 *   - the target columns to be filled by find operation
 *   - the source columns to generate key/value on put operation
 * This object has common static information and dynamically changing parts are separated as write_primary_context.
 *
 * The member functions whose name begins with `encode` store the encoded key/values in the context working buffer(
 * write_primary_context::encoded_key_ or write_primary_context::encoded_value_).
 */
class write_primary_target {
public:

    friend class write_primary_context;

    /**
     * @brief field mapping type
     * @details list of fields that composes the key or value record
     */
    using field_mapping_type = std::vector<index::field_info>;

    using memory_resource = memory::lifo_paged_memory_resource;
    using variable = takatori::descriptor::variable;

    /**
     * @brief create empty object
     */
    write_primary_target() = default;

    /**
     * @brief create new object
     * @param storage_name the primary storage name to write
     * @param key_meta metadata for keys
     * @param value_meta metadata for values
     * @param input_keys field offset information for incoming key fields
     * @param extracted_keys field offset information for extracted key fields
     * @param extracted_values field offset information for extracted value fields
     */
    write_primary_target(
        std::string_view storage_name,
        maybe_shared_ptr<meta::record_meta> key_meta,
        maybe_shared_ptr<meta::record_meta> value_meta,
        field_mapping_type input_keys,
        field_mapping_type extracted_keys,
        field_mapping_type extracted_values
    );

    ~write_primary_target() = default;
    write_primary_target(write_primary_target const& other) = default;
    write_primary_target& operator=(write_primary_target const& other) = default;
    write_primary_target(write_primary_target&& other) noexcept = default;
    write_primary_target& operator=(write_primary_target&& other) noexcept = default;

    /**
     * @brief create new object from takatori columns
     * @param idx target index information
     * @param keys takatori write keys information
     * @param input_variable_info variable table info for the input variables
     */
    template<class Column>
    write_primary_target(
        yugawara::storage::index const& idx,
        sequence_view<Column const> keys,
        variable_table_info const& input_variable_info
    ) :
        write_primary_target(
            idx.simple_name(),
            index::create_meta(idx, true),
            index::create_meta(idx, false),
            index::create_fields(idx, keys, input_variable_info, true, false),
            index::index_fields(idx, true),
            index::index_fields(idx, false)
        ) {}

    /**
     * @brief encode key (stored in context), find the record, fill dest key/value records, and remove
     * @param ctx context
     * @param tx transaction context
     * @param key key record to fine entry in the primary index
     * @param varlen_resource resource for variable length data
     * @param dest_key [out] extracted key record
     * @param dest_value [out] extracted value record
     * @returns status::ok when successful
     * @returns status::not_found if record is not found
     * @returns any other error otherwise
     */
    status encode_find_remove(
        write_primary_context& ctx,
        transaction_context& tx,
        accessor::record_ref key,
        memory_resource* varlen_resource,
        accessor::record_ref dest_key,
        accessor::record_ref dest_value
    );

    /**
     * @brief encode key (store in context), find the record, and fill dest key/value records
     * @param ctx context
     * @param tx transaction context
     * @param key key record to fine entry in the primary index
     * @param varlen_resource resource for variable length data
     * @param dest_key [out] extracted key record
     * @param dest_value [out] extracted value record
     * @returns status::ok when successful
     * @returns status::not_found if record is not found
     * @returns any other error otherwise
     */
    status encode_find(
        write_primary_context& ctx,
        transaction_context& tx,
        accessor::record_ref key,
        memory_resource* varlen_resource,
        accessor::record_ref dest_key,
        accessor::record_ref dest_value
    );

    /**
     * @brief same as `encode_find`, except returning view of encoded key for recycle.
     * @param ctx context
     * @param tx transaction context
     * @param key key record to fine entry in the primary index
     * @param varlen_resource resource for variable length data
     * @param dest_key [out] extracted key record
     * @param dest_value [out] extracted value record
     * @param encoded_key [out] created encoded key (stored internally)
     * @returns status::ok when successful
     * @returns status::not_found if record is not found
     * @returns any other error otherwise
     */
    status encode_find(
        write_primary_context& ctx,
        transaction_context& tx,
        accessor::record_ref key,
        memory_resource* varlen_resource,
        accessor::record_ref dest_key,
        accessor::record_ref dest_value,
        std::string_view& encoded_key
    );

    /**
     * @brief encode key (store in context), and remove the record
     * @returns status::ok when successful
     * @returns status::not_found if record is not found
     * @returns any other error otherwise
     */
    status encode_remove(
        write_primary_context& ctx,
        transaction_context& tx,
        accessor::record_ref key
    );

    /**
     * @brief remove the record by finding entry with encoded key
     * @returns status::ok when successful
     * @returns status::not_found if record is not found
     * @returns any other error otherwise
     */
    status remove_by_encoded_key(
        write_primary_context& ctx,
        transaction_context& tx,
        std::string_view encoded_key
    );

    /**
     * @brief encode key/value (store in context) from the given key/value records, and put them to index
     * @returns status::ok when successful
     * @returns status::already_exist if record already exists and `opt` is `create`
     * @returns status::not_found if record not found and `opt` is `update`
     * @returns any other error otherwise
     */
    status encode_put(
        write_primary_context& ctx,
        transaction_context& tx,
        kvs::put_option opt,
        accessor::record_ref key_record,
        accessor::record_ref value_record,
        std::string_view& encoded_key
        );

    /**
     * @brief accessor to key metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& key_meta() const noexcept;

    /**
     * @brief accessor to value metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& value_meta() const noexcept;

    /**
     * @brief accessor to storage name
     */
    [[nodiscard]] std::string_view storage_name() const noexcept;

private:

    std::string storage_name_{};
    maybe_shared_ptr<meta::record_meta> key_meta_{};
    maybe_shared_ptr<meta::record_meta> value_meta_{};
    field_mapping_type input_keys_{};
    field_mapping_type extracted_keys_{};
    field_mapping_type extracted_values_{};


    status decode_fields(
        field_mapping_type const& fields,
        kvs::readable_stream& stream,
        accessor::record_ref target,
        memory_resource* varlen_resource
    ) const;

    /**
     * @brief encode key on `ctx.encoded_key_` and return its view
     * @param ctx context
     * @param source source record to encode key
     * @param out [out] generated encode key
     * @return
     */
    status prepare_encoded_key(write_primary_context& ctx, accessor::record_ref source, std::string_view& out) const;

};

}  // namespace jogasaki::index
