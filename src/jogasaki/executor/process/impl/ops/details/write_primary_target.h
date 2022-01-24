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

#include <vector>

#include <yugawara/storage/index.h>
#include <takatori/relation/write.h>
#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/details/field_info.h>
#include <jogasaki/kvs/coder.h>
#include "write_primary_context.h"

namespace jogasaki::executor::process::impl::ops::details {

using takatori::util::maybe_shared_ptr;

/**
 * @brief undefined offset value
 */
static constexpr std::size_t npos = static_cast<std::size_t>(-1);

/**
 * @brief field info of the update operation
 * @details update operation uses these fields to know how the variables or input record fields are are mapped to
 * key/value fields. The update operation retrieves the key/value records from kvs and decode to
 * the record (of key/value respectively), updates the record fields by replacing the value with one from variable table
 * record (source), encodes the record and puts into kvs.
 */
struct cache_align update_field {
    /**
     * @brief create new object
     * @param type type of the field
     * @param source_offset byte offset of the field in the input variables record (in variable table)
     * @param source_nullity_offset bit offset of the field nullity in the input variables record
     * @param target_offset byte offset of the field in the target record in ctx.key_store_/value_store_.
     * @param target_nullity_offset bit offset of the field nullity in the target record in ctx.key_store_/value_store_.
     * @param nullable whether the target field is nullable or not
     * @param source_external indicates whether the source is from host variables
     * @param key indicates the filed is part of the key
     */
    update_field(
        meta::field_type type,
        std::size_t source_offset,
        std::size_t source_nullity_offset,
        std::size_t target_offset,
        std::size_t target_nullity_offset,
        bool nullable,
        bool source_external,
        bool key
    ) :
        type_(std::move(type)),
        source_offset_(source_offset),
        source_nullity_offset_(source_nullity_offset),
        target_offset_(target_offset),
        target_nullity_offset_(target_nullity_offset),
        nullable_(nullable),
        source_external_(source_external),
        key_(key)
    {}
    meta::field_type type_{};  //NOLINT
    std::size_t source_offset_{};  //NOLINT
    std::size_t source_nullity_offset_{};  //NOLINT
    std::size_t target_offset_{};  //NOLINT
    std::size_t target_nullity_offset_{};  //NOLINT
    bool nullable_{}; //NOLINT
    bool source_external_{}; //NOLINT
    bool key_{}; //NOLINT
};

/**
 * @brief primary target for write
 * @details this object represents write operation interface for primary index
 */
class write_primary_target {
public:
    friend class write_primary_context;

    using key = takatori::relation::write::key;
    using column = takatori::relation::write::column;
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
     * @param updates update information such as source/target field offsets
     */
    write_primary_target(
        std::string_view storage_name,
        maybe_shared_ptr<meta::record_meta> key_meta,
        maybe_shared_ptr<meta::record_meta> value_meta,
        std::vector<details::field_info> input_keys,
        std::vector<details::field_info> extracted_keys,
        std::vector<details::field_info> extracted_values,
        std::vector<details::update_field> updates
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
     * @param columns takatori write columns information
     * @param input_variable_info variable table info for the input variables
     * @param host_variable_info host variable info used as source for update.
     */
    write_primary_target(
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns,
        variable_table_info const& input_variable_info,
        variable_table_info const* host_variable_info = nullptr
    );

    /**
     * @brief encode key, find the record, fill variables, and remove
     */
    status find_record_and_remove(
        write_primary_context& ctx,
        transaction_context& tx,
        accessor::record_ref variables,
        memory_resource* varlen_resource
    );

    /**
     * @brief do update by copying values from source variable(or host variables) to target.
     */
    void update_record(
        write_primary_context& ctx,
        accessor::record_ref input_variables,
        accessor::record_ref host_variables
    );

    /**
     * @brief gather the extracted (possibly updated) variables, encode key/value and put them to index
     */
    status encode_and_put(write_primary_context& ctx, transaction_context& tx) const;

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
    std::vector<details::field_info> input_keys_{};
    std::vector<details::field_info> extracted_keys_{};
    std::vector<details::field_info> extracted_values_{};
    std::vector<details::update_field> updates_{};

    status encode_fields(
        std::vector<details::field_info> const& fields,
        kvs::writable_stream& target,
        accessor::record_ref source
    ) const;

    std::vector<details::field_info> create_input_key_fields(
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        variable_table_info const& input_variable_info
    );

    std::vector<details::field_info> create_extracted_fields(
        yugawara::storage::index const& idx,
        bool key
    );

    std::vector<details::update_field> create_update_fields(
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns,
        variable_table_info const* host_variable_info,
        variable_table_info const& input_variable_info
    );

    status check_length_and_extend_buffer(
        std::vector<details::field_info> const& fields,
        data::aligned_buffer& buffer,
        accessor::record_ref source
    ) const;

    void decode_fields(
        std::vector<details::field_info> const& fields,
        kvs::readable_stream& stream,
        accessor::record_ref target,
        memory_resource* varlen_resource
    ) const;

    status prepare_encoded_key(write_primary_context& ctx, accessor::record_ref source, std::string_view& out) const;
};

}