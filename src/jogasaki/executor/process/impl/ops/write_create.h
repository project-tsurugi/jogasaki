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

#include <cstddef>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <takatori/relation/write.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/storage/index.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/executor/conv/assignment.h>
#include <jogasaki/executor/insert/insert_new_record.h>
#include <jogasaki/executor/insert/write_field.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/ops/write_partial.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/index/primary_target.h>
#include <jogasaki/index/secondary_target.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/utils/interference_size.h>

#include "write_create_context.h"
#include "write_kind.h"

namespace jogasaki::executor::process::impl::ops {



/**
 * @brief write operator to create new record
 * @details write operator that create new record. Used for Insert/Upsert operations.
 */
class write_create : public record_operator {
public:
    friend class write_create_context;

    using key = takatori::relation::write::key;
    using column = takatori::relation::write::column;
    using memory_resource = memory::lifo_paged_memory_resource;
    using bool_list_type = std::basic_string<bool>; // to utilize sso

    /**
     * @brief create empty object
     */
    write_create() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param kind write operation kind
     * @param primary the primary target of this write operation
     * @param secondaries the secondary targets of this write operation
     * @param input_variable_info input variable information
     */
    write_create(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        write_kind kind,
        yugawara::storage::index const& idx,
        sequence_view<column const> columns,
        memory::lifo_paged_memory_resource* resource,
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
    operation_status operator()(write_create_context& ctx);

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
    [[nodiscard]] index::primary_target const& primary() const noexcept;

private:
    write_kind kind_{};
    maybe_shared_ptr<meta::record_meta> key_meta_{};
    maybe_shared_ptr<meta::record_meta> value_meta_{};
    std::vector<insert::write_field> key_fields_{};
    std::vector<insert::write_field> value_fields_{};
    std::shared_ptr<insert::insert_new_record> core_{};
    std::vector<details::update_field> update_fields_{};
};

}  // namespace jogasaki::executor::process::impl::ops
