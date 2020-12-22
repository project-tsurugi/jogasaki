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

#include <takatori/relation/write.h>
#include <yugawara/storage/index.h>

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/kvs/coder.h>
#include "write_full_context.h"
#include "write_kind.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief field info of the write operation
 * @details write operator uses these fields to know how the scope variables or input record fields are are mapped to
 * key/value fields.
 */
struct cache_align write_full_field {
    /**
     * @brief create new write field
     * @param type type of the write field
     * @param source_offset byte offset of the source field in the source record reference
     * @param source_nullity_offset bit offset of the source field nullity in the source record reference
     * @param source_nullable whether the target field is nullable or not
     * @param spec the spec of the source field used for encode/decode
     */
    write_full_field(
        meta::field_type type,
        std::size_t source_offset,
        std::size_t source_nullity_offset,
        bool target_nullable,
        kvs::coding_spec spec
    );

    meta::field_type type_{}; //NOLINT
    std::size_t source_offset_{}; //NOLINT
    std::size_t source_nullity_offset_{}; //NOLINT
    bool target_nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
};

}

/**
 * @brief full write operator
 * @details write operator that fully specifies the data to target columns. Used for insert/upsert/delete operations.
 */
class write_full : public record_operator {
public:
    friend class write_full_context;

    using key = takatori::relation::write::key;
    using column = takatori::relation::write::column;
    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    write_full() = default;

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
    write_full(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        std::string_view storage_name,
        std::vector<details::write_full_field> key_fields,
        std::vector<details::write_full_field> value_fields
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
    write_full(
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
    void operator()(write_full_context& ctx);

    [[nodiscard]] operator_kind kind() const noexcept override;
    /**
     * @brief return storage name
     * @return the storage name of the write target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept;

    void finish(abstract::task_context* context) override;
private:
    write_kind kind_{};
    std::string storage_name_{};
    std::vector<details::write_full_field> key_fields_{};
    std::vector<details::write_full_field> value_fields_{};

    void encode_fields(
        std::vector<details::write_full_field> const& fields,
        kvs::stream& stream,
        accessor::record_ref source,
        memory_resource* resource
    );

    std::vector<details::write_full_field> create_fields(
        write_kind kind,
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns,
        processor_info const& info,
        block_index_type block_index,
        bool key
    );

    void check_length_and_extend_buffer(
        write_full_context& ctx,
        std::vector<details::write_full_field> const& fields,
        data::aligned_buffer& buffer,
        accessor::record_ref source,
        memory_resource* resource
    );

    void do_insert(write_kind kind, write_full_context& ctx);

    std::string_view prepare_key(write_full_context& ctx);

    void do_delete(write_full_context& ctx);
};

}


