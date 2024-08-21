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

#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <takatori/relation/scan.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/storage/index.h>

#include <jogasaki/executor/process/abstract/scan_info.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/checkpoint_holder.h>

#include "index_field_mapper.h"
#include "operator_base.h"
#include "scan_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

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
        std::string_view secondary_storage_name,
        std::vector<index::field_info> key_fields,
        std::vector<index::field_info> value_fields,
        std::vector<details::secondary_index_field_info> secondary_key_fields,
        std::unique_ptr<operator_base> downstream = nullptr,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    );

    /**
     * @brief create new object from takatori columns
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param primary_idx the primary index model used as the first step to find entry
     * @param columns takatori scan column information
     * @param secondary_idx the secondary index used to find the primary key.
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    scan(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        yugawara::storage::index const& primary_idx,
        sequence_view<column const> columns,
        yugawara::storage::index const* secondary_idx,
        std::unique_ptr<operator_base> downstream,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
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
     * @brief return secondary index storage name
     * @return the secondary index storage name of the scan target
     */
    [[nodiscard]] std::string_view secondary_storage_name() const noexcept;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context*) override;;

    /**
     * @brief Support for debugging, callable in GDB: ctx->dump()
     */
    void dump() const noexcept;

private:
    bool use_secondary_{};
    std::string storage_name_{};
    std::string secondary_storage_name_{};
    std::unique_ptr<operator_base> downstream_{};
    index_field_mapper field_mapper_{};

    [[nodiscard]] status open(scan_context& ctx);

    void close(scan_context& ctx);

    std::vector<details::secondary_index_field_info> create_secondary_key_fields(
        yugawara::storage::index const* idx
    );
    std::size_t maxIterations_ = 0;
};


}


