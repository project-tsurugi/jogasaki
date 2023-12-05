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
#include <yugawara/storage/index.h>

#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/kvs/coder.h>

#include "details/write_primary_target.h"
#include "details/write_secondary_target.h"
#include "write_kind.h"
#include "write_partial_context.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

/**
 * @brief field info of the update operation
 * @details update operation uses these fields to know how the variables or input record fields are mapped to
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
     * @param target_offset byte offset of the field in the target record in
     * ctx.extracted_key_store_/extracted_value_store_.
     * @param target_nullity_offset bit offset of the field nullity in the target record in
     * ctx.extracted_key_store_/extracted_value_store_.
     * @param nullable whether the target field is nullable or not
     * @param source_external indicates whether the source is from host variables
     * @param key indicates the fieled is part of the key
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

}  // namespace details

/**
 * @brief partial write operator
 * @details write operator that partially specifies the data to target columns. Used for Update/Delete operation.
 */
class write_partial : public record_operator {
public:
    friend class write_partial_context;

    using key = takatori::relation::write::key;
    using column = takatori::relation::write::column;
    using memory_resource = memory::lifo_paged_memory_resource;
    using bool_list_type = std::basic_string<bool>; // to utilize sso

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
     * @param primary the primary target of this write operation
     * @param updates the updated field definition
     * @param secondaries the secondary targets of this write operation
     * @param secondary_key_updated list of flags indicating if one of index keys of secondary targets are updated
     * @param input_variable_info input variable information
     */
    write_partial(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        details::write_primary_target primary,
        std::vector<details::update_field> updates,
        std::vector<details::write_secondary_target> secondaries,
        bool_list_type secondary_key_updated,
        variable_table_info const* input_variable_info = nullptr
    );

    /**
     * @brief create new object from takatori columns
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param kind write operation kind
     * @param idx the primary index that this write operation depends (secondaries under this primary are also handled)
     * @param keys takatori write keys information in the sense of primary index
     * @param columns takatori write columns information
     * @param input_variable_info input variable information
     */
    write_partial(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        yugawara::storage::index const& idx,
        sequence_view<key const> keys,
        sequence_view<column const> columns,
        variable_table_info const* input_variable_info = nullptr
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
     * @brief return primary index storage name
     * @return the storage name of the write target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

    /**
     * @brief accessor to primary target
     */
    [[nodiscard]] details::write_primary_target const& primary() const noexcept;

private:

    write_kind kind_{};
    details::write_primary_target primary_{};
    std::vector<details::write_secondary_target> secondaries_{};
    bool primary_key_updated_{};
    bool_list_type secondary_key_updated_{};
    std::vector<details::update_field> updates_{};

    operation_status do_update(write_partial_context& ctx);
    operation_status do_delete(write_partial_context& ctx);
    void update_record(
        accessor::record_ref extracted_key_record,
        accessor::record_ref extracted_value_record,
        accessor::record_ref input_variables,
        accessor::record_ref host_variables
    );
};

}  // namespace jogasaki::executor::process::impl::ops
