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

#include <takatori/util/sequence_view.h>

#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/cogroup.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include "operator_kind.h"

namespace jogasaki::executor::process::impl {
class variable_table_info;
}

namespace jogasaki::executor::process::impl::ops {

/**
 * @brief relational operator base class
 */
class operator_base {
public:
    /**
     * @brief block index identifies the basic block where this operator belongs
     */
    using block_index_type = std::size_t;

    /**
     * @brief operator index is the unique identifier of the operator within the process
     */
    using operator_index_type = std::size_t;

    /**
     * @brief undefined position constant
     */
    static constexpr std::size_t npos = static_cast<std::size_t>(-1);

    /**
     * @brief create empty object
     */
    operator_base() = default;

    /**
     * @brief destruct the object
     */
    virtual ~operator_base() = default;

    /**
     * @brief create new object
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param input_variable_info the input variable table information optionally overriding the one in processor_info.
     * @param output_variable_info the output variable table information optionally overriding the one in processor_info.
     */
    operator_base(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    ) noexcept;

    operator_base(operator_base const& other) = default;
    operator_base& operator=(operator_base const& other) = default;
    operator_base(operator_base&& other) noexcept = default;
    operator_base& operator=(operator_base&& other) noexcept = default;

    /**
     * @brief return the kind of the operator
     */
    [[nodiscard]] virtual operator_kind kind() const noexcept = 0;

    /**
     * @brief return the block variables information, where the operator belongs
     */
    [[nodiscard]] variable_table_info const& block_info() const noexcept;

    /**
     * @brief return the block variables information, where the operator belongs
     */
    [[nodiscard]] variable_table_info const& output_variable_info() const noexcept;

    /**
     * @brief return the block index where the operator belongs
     */
    [[nodiscard]] block_index_type block_index() const noexcept;

    /**
     * @brief accessor to the compiled info
     */
    [[nodiscard]] yugawara::compiled_info const& compiled_info() const noexcept;

    /**
     * @brief accessor to the operator index within the process
     */
    [[nodiscard]] operator_index_type index() const noexcept;

    /**
     * @brief accessor to the host variables
     */
    [[nodiscard]] variable_table const* host_variables() const noexcept;

    /**
     * @brief tell the operator to finish processing
     * @details This function notifies the operators the end of processing. This is typically called by top operator
     * in the process by propagating the notice to downstream. The operator can use function to clean-up work
     * such as flushing buffers.
     * @param context the task context
     */
    virtual void finish(abstract::task_context* context) = 0;

private:
    operator_index_type index_{};
    processor_info const* processor_info_{};
    block_index_type block_index_{};
    variable_table_info const* input_variable_info_{};
    variable_table_info const* output_variable_info_{};
};

/**
 * @brief operator receiving flat record
 */
class record_operator : public operator_base {
public:
    record_operator() = default;

    record_operator(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    ) noexcept;

    /**
     * @brief process a record
     * @param context the task context to process
     * @return status of the operation
     */
    virtual operation_status process_record(abstract::task_context* context) = 0;
};

/**
 * @brief operator receiving group
 */
class group_operator : public operator_base {
public:
    group_operator() = default;

    group_operator(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index
    ) noexcept;

    /**
     * @brief process a record that composes the group
     * @details this function is called for each record in the group and the `last_member` flag indicates
     * if the current group is finished.
     * @param context the task context to process
     * @param last_member specify whether the current member is the last within the group
     * @return status of the operation
     */
    virtual operation_status process_group(abstract::task_context* context, bool last_member) = 0;
};

/**
 * @brief operator receiving cogroup
 * @tparam Iterator the iterator type used to iterate records from the group member
 */
template<class Iterator>
class cogroup_operator : public operator_base {
public:
    cogroup_operator() = default;

    cogroup_operator(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index
    ) noexcept :
        operator_base(index, info, block_index)
    {}

    /**
     * @brief process a cogroup
     * @details this function is called for each cogroup generated by the upstream operator
     * @param context the task context to process
     * @param cgrp the cogroup to be processed
     * @return status of the operation
     */
    virtual operation_status process_cogroup(abstract::task_context* context, cogroup<Iterator>& cgrp) = 0;
};

}
