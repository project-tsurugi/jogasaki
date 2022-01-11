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
struct cache_align write_partial_field {
    /**
     * @brief create new object
     * @param type type of the field
     * @param variable_offset byte offset of the field in the input variables record (in variable table)
     * @param variable_nullity_offset bit offset of the field nullity in the input variables record
     * @param target_offset byte offset of the field in the target record in ctx.key_store_/value_store_.
     * @param target_nullity_offset bit offset of the field nullity in the target record in ctx.key_store_/value_store_.
     * @param nullable whether the target field is nullable or not
     * @param spec the spec of the source field used for encode/decode
     * @param updated indicates whether the field will be updated or not
     * @param update_variable_offset byte offset of the field in the variable table.
     * Used to provide values only if `updated` is true.
     * @param update_variable_nullity_offset bit offset of the field nullity in the variable table.
     * Used to provide values nullity only if `updated` is true.
     * @param update_variable_is_external indicates whether the update_variable source is from host variables
     */
    write_partial_field(
        meta::field_type type,
        std::size_t variable_offset,
        std::size_t variable_nullity_offset,
        std::size_t target_offset,
        std::size_t target_nullity_offset,
        bool nullable,
        kvs::coding_spec spec,
        bool updated,
        std::size_t update_variable_offset,
        std::size_t update_variable_nullity_offset,
        bool update_variable_is_external
    );

    meta::field_type type_{}; //NOLINT
    std::size_t variable_offset_{}; //NOLINT
    std::size_t variable_nullity_offset_{}; //NOLINT
    std::size_t target_offset_{}; //NOLINT
    std::size_t target_nullity_offset_{}; //NOLINT
    bool nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
    bool updated_{}; //NOLINT
    std::size_t update_variable_offset_{}; //NOLINT
    std::size_t update_variable_nullity_offset_{}; //NOLINT
    bool update_variable_is_external_{}; //NOLINT
};

class primary_target {
public:
    friend class primary_target_context;

    using key = takatori::relation::write::key;
    using column = takatori::relation::write::column;
    using memory_resource = memory::lifo_paged_memory_resource;
    using variable = takatori::descriptor::variable;

    /**
     * @brief create empty object
     */
    primary_target() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param kind write operation kind
     * @param storage_name the storage name to write
     * @param key_fields field offset information for keys
     * @param value_fields field offset information for values
     */
    primary_target(
        std::string_view storage_name,
        std::vector<details::write_partial_field> key_fields,
        std::vector<details::write_partial_field> value_fields,
        maybe_shared_ptr<meta::record_meta> key_meta,
        maybe_shared_ptr<meta::record_meta> value_meta
    );

    ~primary_target() = default;
    primary_target(primary_target const& other) = default;
    primary_target& operator=(primary_target const& other) = default;
    primary_target(primary_target&& other) noexcept = default;
    primary_target& operator=(primary_target&& other) noexcept = default;

    /**
     * @brief create new object from takatori columns
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param kind write operation kind
     * @param storage_name the storage name to write
     * @param idx target index information
     * @param keys takatori write keys information
     * @param columns takatori write columns information
     */
    primary_target(
        std::string_view storage_name,
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* host_variable_info = nullptr
    );

    status find_record_and_extract(
        primary_target_context& ctx,
        kvs::transaction& tx,
        accessor::record_ref variables,
        memory_resource* varlen_resource
    );

    void update_record(
        primary_target_context& ctx,
        accessor::record_ref input_variables,
        accessor::record_ref host_variables
    );

    status encode_and_put(primary_target_context& ctx, kvs::transaction& tx) const;

    /**
     * @brief accessor to key metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& key_meta() const noexcept {
        return key_meta_;
    }

    /**
     * @brief accessor to value metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& value_meta() const noexcept {
        return value_meta_;
    }

    [[nodiscard]] std::string_view storage_name() const noexcept {
        return storage_name_;
    }
private:
    std::string storage_name_{};
    std::vector<details::write_partial_field> key_fields_{};
    std::vector<details::write_partial_field> value_fields_{};
    maybe_shared_ptr<meta::record_meta> key_meta_{};
    maybe_shared_ptr<meta::record_meta> value_meta_{};

    /**
     * @brief private encoding function
     * @param from_variable specify where the source comes from. True when encoding from variable table to
     * internal buffer (key_buf_/value_buf_). False when encoding internal buffer to kvs::writable_streams.
     * @returns status::ok if successful
     * @returns error otherwise
     */
    status encode_fields(
        bool from_variable,
        std::vector<details::write_partial_field> const& fields,
        kvs::writable_stream& target,
        accessor::record_ref source
    ) const;

    // create meta for the key_store_/value_store_ in write_partial_context
    maybe_shared_ptr<meta::record_meta> create_meta(yugawara::storage::index const& idx, bool for_key);

    std::vector<details::write_partial_field> create_fields(
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns,
        variable_table_info const* host_variable_info,
        variable_table_info const& input_variable_info,
        bool key
    );

    status check_length_and_extend_buffer(
        bool from_variables,
        primary_target_context& ctx,
        std::vector<details::write_partial_field> const& fields,
        data::aligned_buffer& buffer,
        accessor::record_ref source
    ) const;

    void decode_fields(
        std::vector<details::write_partial_field> const& fields,
        kvs::readable_stream& stream,
        accessor::record_ref target,
        memory_resource* varlen_resource
    ) const;

    void update_fields(
        std::vector<details::write_partial_field> const& fields,
        accessor::record_ref target,
        accessor::record_ref input_variables,
        accessor::record_ref host_variables
    ) const;

    status prepare_encoded_key(primary_target_context& ctx, accessor::record_ref source, std::string_view& out) const;
};

}