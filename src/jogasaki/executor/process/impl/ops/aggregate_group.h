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
#include <unordered_map>
#include <utility>
#include <vector>

#include <takatori/descriptor/variable.h>
#include <takatori/relation/step/aggregate.h>
#include <takatori/util/downcast.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/binding/extract.h>

#include <jogasaki/executor/function/aggregate_function_info.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/utils/interference_size.h>

#include "aggregate_group_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;
using takatori::util::sequence_view;

namespace details {

/**
 * @brief column generated as the result of aggregate group operation
 */
class cache_align aggregate_group_column {
public:
    aggregate_group_column(
        meta::field_type type,
        std::vector<std::size_t> argument_indices,
        function::aggregate_function_info function_info,
        std::size_t offset,
        std::size_t nullity_offset,
        bool nullable
    );

    meta::field_type type_{};  //NOLINT
    std::vector<std::size_t> argument_indices_{};  //NOLINT
    function::aggregate_function_info function_info_{};  //NOLINT
    std::size_t offset_{};  //NOLINT
    std::size_t nullity_offset_{};  //NOLINT
    bool nullable_{};  //NOLINT
};

/**
 * @brief aggregate function argument used within aggregate_group
 */
class cache_align aggregate_group_argument {
public:
    aggregate_group_argument(
        meta::field_type type,
        std::size_t offset,
        std::size_t nullity_offset,
        bool nullable
    ) noexcept;

    meta::field_type type_{};  //NOLINT
    std::size_t offset_{};  //NOLINT
    std::size_t nullity_offset_{};  //NOLINT
    bool nullable_{};  //NOLINT
};

}
/**
 * @brief aggregate_group operator
 */
class aggregate_group : public group_operator {
public:
    friend class aggregate_group_context;

    using column = takatori::relation::step::aggregate::column;
    /**
     * @brief create empty object
     */
    aggregate_group() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param columns takatori aggregate columns definitions
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    aggregate_group(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        sequence_view<column const> columns,
        std::unique_ptr<operator_base> downstream = nullptr
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     * @param last_member specify whether the current member is the last within the group
     * @return status of the operation
     */
    operation_status process_group(abstract::task_context* context, bool last_member) override;

    /**
     * @brief process record with context object
     * @details this operation is almost no-op because take_group already took records and assigned variables
     * @param ctx context object for the execution
     * @param last_member specify whether the current member is the last within the group
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(
        aggregate_group_context& ctx,
        bool last_member,
        abstract::task_context* context = nullptr
    );

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override;

private:
    std::unique_ptr<operator_base> downstream_{};
    std::vector<details::aggregate_group_column> columns_{};
    std::vector<details::aggregate_group_argument> arguments_{};

    std::vector<details::aggregate_group_column> create_columns(
        sequence_view<column const> columns
    );

    std::vector<details::aggregate_group_argument> create_arguments(
        sequence_view<column const> columns
    );

    std::pair<
        std::vector<takatori::descriptor::variable>,
        std::unordered_map<takatori::descriptor::variable, std::size_t>
    >
    variable_indices(sequence_view<column const> columns);

    aggregate_group_context* create_context_if_not_found(abstract::task_context* context);
};

}


