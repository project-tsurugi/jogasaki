/*
 * Copyright 2018-2024 Project Tsurugi.
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
#include <memory>
#include <vector>

#include <takatori/relation/step/take_group.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/meta/variable_order.h>
#include <jogasaki/utils/interference_size.h>

#include "operator_base.h"
#include "take_group_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

namespace details {

struct cache_align take_group_field {
    meta::field_type type_{};
    std::size_t source_offset_{};
    std::size_t target_offset_{};
    std::size_t source_nullity_offset_{};
    std::size_t target_nullity_offset_{};
    bool nullable_{};
    bool is_key_{};
};

}

/**
 * @brief take_group operator
 */
class take_group : public record_operator {
public:
    using column = takatori::relation::step::take_group::column;

    friend class take_group_context;
    /**
     * @brief create empty object
     */
    take_group() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param order the exchange columns ordering information that assigns the field index of the input record.
     * The index can be used with record_meta to get field metadata.
     * @param meta the metadata of the group
     * @param reader_index the index that identifies the reader in the task context. This corresponds to the input port
     * number that the input exchange is connected.
     * @param downstream downstream operator that should be invoked with the output from this operation
     */
    take_group(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        meta::variable_order const& order,
        maybe_shared_ptr<meta::group_meta> meta,
        takatori::util::sequence_view<column const> columns,
        std::size_t reader_index,
        std::unique_ptr<operator_base> downstream = nullptr
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @return status of the operation
     */
    operation_status process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details process record, fill variables, and invoke downstream
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(take_group_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @brief accessor to group metadata
     */
    [[nodiscard]] maybe_shared_ptr<meta::group_meta> const& meta() const noexcept;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context*) override;

private:
    maybe_shared_ptr<meta::group_meta> meta_{};
    std::vector<details::take_group_field> fields_{};
    std::size_t reader_index_{};
    std::unique_ptr<operator_base> downstream_{};

    [[nodiscard]] std::vector<details::take_group_field> create_fields(
        maybe_shared_ptr<meta::group_meta> const& meta,
        meta::variable_order const& order,
        takatori::util::sequence_view<column const> columns
    );
};

}


