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

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/kvs/coder.h>
#include "write_partial_context.h"
#include "write_kind.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief field info of the update operation
 * @details update operation uses these fields to know how the scope variables or input record fields are are mapped to
 * key/value fields. The update operation retrieves the key/value records from kvs and decode to
 * the record (of key/value respectively), updates the record fields by replacing the value with one from scope variable
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
     * @param source_offset byte offset of the field in the input variables record
     * @param source_nullity_offset bit offset of the field nullity in the input variables record
     * @param target_offset byte offset of the field in the decoded record
     * @param target_nullity_offset bit offset of the field nullity in the decoded record
     * @param nullable whether the target field is nullable or not
     * @param spec the spec of the source field used for encode/decode
     * @param updated indicates whether the field will be updated or not
     * @param update_source_offset byte offset of the field in the source variables record (used if updated is true)
     * @param update_source_nullity_offset bit offset of the field nullity in the source variables record (used if updated is true)
     */
    write_partial_field(
        meta::field_type type,
        std::size_t source_offset,
        std::size_t source_nullity_offset,
        std::size_t target_offset,
        std::size_t target_nullity_offset,
        bool nullable,
        kvs::coding_spec spec,
        bool updated,
        std::size_t update_source_offset,
        std::size_t update_source_nullity_offset
    );

    meta::field_type type_{}; //NOLINT
    std::size_t source_offset_{}; //NOLINT
    std::size_t source_nullity_offset_{}; //NOLINT
    std::size_t target_offset_{}; //NOLINT
    std::size_t target_nullity_offset_{}; //NOLINT
    bool nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
    bool updated_{}; //NOLINT
    std::size_t update_source_offset_{}; //NOLINT
    std::size_t update_source_nullity_offset_{}; //NOLINT
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
     */
    void process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details process record, construct key/value sequences and invoke kvs to conduct write operations
     * @param ctx operator context object for the execution
     */
    void operator()(write_partial_context& ctx);

    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @brief return storage name
     * @return the storage name of the write target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept;

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

    void encode_fields(
        bool from_variable,
        std::vector<details::write_partial_field> const& fields,
        kvs::stream& stream,
        accessor::record_ref source,
        memory_resource* resource
    );

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
        accessor::record_ref source,
        memory_resource* resource
    );

    void decode_fields(
        std::vector<details::write_partial_field> const& fields,
        kvs::stream& stream,
        accessor::record_ref target,
        memory_resource* resource
    );

    void update_fields(
        std::vector<details::write_partial_field> const& fields,
        accessor::record_ref target,
        accessor::record_ref source
    );

    void find_record_and_extract(write_partial_context& ctx);

    void update_record(write_partial_context& ctx);

    void encode_and_put(write_partial_context& ctx);

    std::string_view prepare_encoded_key(write_partial_context& ctx);
};

}
