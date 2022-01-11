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
#include "details/write_primary_target.h"

namespace jogasaki::executor::process::impl::ops {

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
     * @param primary the primary target of this write operation
     * @param input_variable_info input variable information
     */
    write_partial(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        details::write_primary_target primary,
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
};

}
