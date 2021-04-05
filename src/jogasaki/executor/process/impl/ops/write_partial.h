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
#include "write_partial_context.h"
#include "write_kind.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;

namespace details {

/**
 * @brief field info of the update operation
 * @details update operation uses these fields to know how the variables or input record fields are are mapped to
 * key/value fields. The update operation retrieves the key/value records from kvs and decode to
 * the record (of key/value respectively), updates the record fields by replacing the value with one from variable table
 * record (source), encodes the record and puts into kvs.
 */
struct cache_align write_partial_field {
    /**
     * @brief undefined offset value
     */
    static constexpr std::size_t npos = static_cast<std::size_t>(-1);

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

}

/**
 * @brief partial write operator
 * @details write operator that partially specifies the data to target columns. Used for Update operation.
 */
class write_partial : public record_operator {
public:
    friend class write_partial_context;

    using key = takatori::relation::write::key;
    using column = takatori::relation::write::column;
    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    write_partial() = default;

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
    write_partial(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        std::string_view storage_name,
        std::vector<details::write_partial_field> key_fields,
        std::vector<details::write_partial_field> value_fields,
        maybe_shared_ptr<meta::record_meta> key_meta,
        maybe_shared_ptr<meta::record_meta> value_meta
    );

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
    write_partial(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        std::string_view storage_name,
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns
    );


    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details process record, construct key/value sequences and invoke kvs to conduct write operations
     * @param ctx operator context object for the execution
     * @return status of the operation
     */
    operation_status operator()(write_partial_context& ctx);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @brief return storage name
     * @return the storage name of the write target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

    /**
     * @brief accessor to key metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& key_meta() const noexcept;

    /**
     * @brief accessor to value metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::record_meta> const& value_meta() const noexcept;
private:
    write_kind kind_{};
    std::string storage_name_{};
    std::vector<details::write_partial_field> key_fields_{};
    std::vector<details::write_partial_field> value_fields_{};
    maybe_shared_ptr<meta::record_meta> key_meta_{};
    maybe_shared_ptr<meta::record_meta> value_meta_{};

    /**
     * @brief private encoding function
     * @param from_variable specify where the source comes from. True when encoding from variable table to
     * internal buffer (key_buf_/value_buf_). False when encoding internal buffer to kvs::streams.
     */
    void encode_fields(
        bool from_variable,
        std::vector<details::write_partial_field> const& fields,
        kvs::stream& target,
        accessor::record_ref source
    );

    // create meta for the key_store_/value_store_ in write_partial_context
    maybe_shared_ptr<meta::record_meta> create_meta(yugawara::storage::index const& idx, bool for_key);

    std::vector<details::write_partial_field> create_fields(
        write_kind kind,
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns,
        processor_info const& info,
        block_index_type block_index,
        bool key
    );

    void check_length_and_extend_buffer(
        bool from_variables,
        write_partial_context& ctx,
        std::vector<details::write_partial_field> const& fields,
        data::aligned_buffer& buffer,
        accessor::record_ref source
    );

    void decode_fields(
        std::vector<details::write_partial_field> const& fields,
        kvs::stream& stream,
        accessor::record_ref target,
        memory_resource* varlen_resource
    );

    void update_fields(
        std::vector<details::write_partial_field> const& fields,
        accessor::record_ref target,
        accessor::record_ref source
    );

    operation_status find_record_and_extract(write_partial_context& ctx);

    void update_record(write_partial_context& ctx);

    operation_status encode_and_put(write_partial_context& ctx);

    std::string_view prepare_encoded_key(write_partial_context& ctx);
};

}
