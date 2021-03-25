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

#include <takatori/relation/scan.h>

#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/kvs/coder.h>
#include "operator_base.h"
#include "scan_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief field info of the scan operation
 * @details scan operator uses these fields to know how the scanned key/value are mapped to variables
 */
struct cache_align scan_field {
    /**
     * @brief create new scan field
     * @param type type of the scanned field
     * @param target_exists whether the target storage exists. If not, there is no room to copy the data to.
     * @param target_offset byte offset of the target field in the target record reference
     * @param target_nullity_offset bit offset of the target field nullity in the target record reference
     * @param source_nullable whether the target field is nullable or not
     * @param spec the spec of the target field used for encode/decode
     */
    scan_field(
        meta::field_type type,
        bool target_exists,
        std::size_t target_offset,
        std::size_t target_nullity_offset,
        bool source_nullable,
        kvs::coding_spec spec
    );

    meta::field_type type_{}; //NOLINT
    bool target_exists_{}; //NOLINT
    std::size_t target_offset_{}; //NOLINT
    std::size_t target_nullity_offset_{}; //NOLINT
    bool source_nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
};

}

/**
 * @brief scan operator
 */
class scan : public record_operator {
public:
    friend class scan_context;

    using column = takatori::relation::scan::column;

    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    scan() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param storage_name the storage name to scan
     * @param key_fields field offset information for keys
     * @param value_fields field offset information for values
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    scan(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::string_view storage_name,
        std::vector<details::scan_field> key_fields,
        std::vector<details::scan_field> value_fields,
        std::unique_ptr<operator_base> downstream = nullptr
    );

    /**
     * @brief create new object from takatori columns
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param storage_name the storage name to scan
     * @param columns takatori scan column information
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    scan(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::string_view storage_name,
        yugawara::storage::index const& idx,
        sequence_view<column const> columns,
        std::unique_ptr<operator_base> downstream
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details process record, fill variables with scanned result, and invoke downstream
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(scan_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @brief return storage name
     * @return the storage name of the scan target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context*) override;;
private:
    std::string storage_name_{};
    std::vector<details::scan_field> key_fields_{};
    std::vector<details::scan_field> value_fields_{};
    std::unique_ptr<operator_base> downstream_{};

    void open(scan_context& ctx);

    void close(scan_context& ctx);

    void decode_fields(
        std::vector<details::scan_field> const& fields,
        kvs::stream& stream,
        accessor::record_ref target,
        memory_resource* resource
    );

    std::vector<details::scan_field> create_fields(
        yugawara::storage::index const& idx,
        sequence_view<column const> columns,
        processor_info const& info,
        block_index_type block_index,
        bool key
    );
};


}


