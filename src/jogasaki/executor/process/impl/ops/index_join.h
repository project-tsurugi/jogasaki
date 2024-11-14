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

#include <takatori/relation/join_find.h>
#include <takatori/relation/join_scan.h>
#include <takatori/scalar/expression.h>
#include <takatori/tree/tree_fragment_vector.h>
#include <takatori/util/optional_ptr.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/storage/index.h>

#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/index/field_factory.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/kvs/coder.h>
#include <jogasaki/kvs/iterator.h>
#include <jogasaki/kvs/storage.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>

#include "details/search_key_field_info.h"
#include "index_field_mapper.h"
#include "index_join_context.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

/**
 * @brief create secondary index key field info. Kept public for testing
 */
std::vector<details::secondary_index_field_info> create_secondary_key_fields(
    yugawara::storage::index const* secondary_idx
);

/**
 * @brief matcher object to encapsulate difference between single result find and multiple result
 */
class matcher {
public:
    using memory_resource = memory::lifo_paged_memory_resource;

    matcher(
        bool use_secondary,
        bool for_join_scan,
        std::vector<details::search_key_field_info> const& key_fields,
        std::vector<details::search_key_field_info> const& begin_fields,
        kvs::end_point_kind begin_endpoint,
        std::vector<details::search_key_field_info> const& end_fields,
        kvs::end_point_kind end_endpoint,
        std::vector<index::field_info> key_columns,
        std::vector<index::field_info> value_columns,
        std::vector<details::secondary_index_field_info> secondary_key_fields
    );

    matcher(
        bool use_secondary,
        std::vector<details::search_key_field_info> const& key_fields,
        std::vector<index::field_info> key_columns,
        std::vector<index::field_info> value_columns,
        std::vector<details::secondary_index_field_info> secondary_key_fields
    );

    matcher(
        bool use_secondary,
        std::vector<details::search_key_field_info> const& begin_fields,
        kvs::end_point_kind begin_endpoint,
        std::vector<details::search_key_field_info> const& end_fields,
        kvs::end_point_kind end_endpoint,
        std::vector<index::field_info> key_columns,
        std::vector<index::field_info> value_columns,
        std::vector<details::secondary_index_field_info> secondary_key_fields
    );

    /**
     * @brief execute the matching for join_find
     * @return true if match is successful
     * @return false if match is not successful, check status with result() function to see if the result is
     * simply not-found or other error happened.
     */
    [[nodiscard]] bool process_find(
        request_context& ctx,
        variable_table& input_variables,
        variable_table& output_variables,
        kvs::storage& primary_stg,
        kvs::storage* secondary_stg,
        memory_resource* resource = nullptr
    );

    /**
     * @brief execute the matching for join_scan
     * @return true if match is successful
     * @return false if match is not successful, check status with result() function to see if the result is
     * simply not-found or other error happened.
     */
    [[nodiscard]] bool process_scan(
        request_context& ctx,
        variable_table& input_variables,
        variable_table& output_variables,
        kvs::storage& primary_stg,
        kvs::storage* secondary_stg,
        memory_resource* resource = nullptr
    );

    /**
     * @brief retrieve next match
     * @return true if match is successful
     * @return false if match is not successful, check status with result() function to see if the result is
     * simply not-found or other error happened.
     */
    bool next();

    /**
     * @brief retrieve the status code of the last match execution
     * @details This function provides the status code to check if the match
     * @return status::ok if match was successful
     * @return status::not_found if match was not successful due to missing target record
     * @return other error (e.g. status::err_aborted_retryable) occurred when accessing kvs
     */
    [[nodiscard]] status result() const noexcept;

private:
    bool use_secondary_{};
    bool for_join_scan_{};
    std::vector<details::search_key_field_info> const& key_fields_;
    std::vector<details::search_key_field_info> const& begin_fields_;
    kvs::end_point_kind begin_endpoint_{};
    std::vector<details::search_key_field_info> const& end_fields_;
    kvs::end_point_kind end_endpoint_{};
    data::aligned_buffer buf_{};
    data::aligned_buffer buf2_{};
    status status_{status::ok};
    index_field_mapper field_mapper_{};

    variable_table* output_variables_{};
    kvs::storage* primary_storage_{};
    transaction_context* tx_{};
    matcher::memory_resource* resource_{};
    std::unique_ptr<kvs::iterator> it_{};
};

}  // namespace details

/**
 * @brief index_join class common for join_find/join_scan operators
 */
class index_join : public record_operator {
public:
    friend class index_join_context;

    using join_kind = takatori::relation::join_kind;

    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    index_join() = default;

    /**
     * @brief common constructor for join_scan and join_find
     * @param kind the kind of the join
     * @param for_join_scan whether this object is used for join_scan (otherwise for join_find)
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param primary_storage_name the storage name to find
     * @param key_columns column information for key fields
     * @param value_columns column information for value fields
     * @param search_key_fields key_field information
     * @param condition additional join condition
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    index_join(
        join_kind kind,
        bool for_join_scan,
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::string_view primary_storage_name,
        std::string_view secondary_storage_name,
        std::vector<index::field_info> key_columns,
        std::vector<index::field_info> value_columns,
        std::vector<details::search_key_field_info> search_key_fields,
        std::vector<details::search_key_field_info> begin_for_scan,
        kvs::end_point_kind begin_endpoint,
        std::vector<details::search_key_field_info> end_for_scan,
        kvs::end_point_kind end_endpoint,
        takatori::util::optional_ptr<takatori::scalar::expression const> condition,
        std::vector<details::secondary_index_field_info> secondary_key_fields,
        std::unique_ptr<operator_base> downstream = nullptr,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    ) noexcept;

    /**
     * @brief create new object for join_find from takatori objects
     * @param kind the kind of the join
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param storage_name the storage name to find
     * @param columns takatori join_find column information
     * @param keys takatori join_find key information
     * @param condition additional join condition
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    index_join(
        join_kind kind,
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        yugawara::storage::index const& primary_idx,
        sequence_view<takatori::relation::join_find::column const> columns,
        takatori::tree::tree_fragment_vector<takatori::relation::join_find::key> const& keys,
        takatori::util::optional_ptr<takatori::scalar::expression const> condition,
        yugawara::storage::index const* secondary_idx,
        std::unique_ptr<operator_base> downstream,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    );

    /**
     * @brief create new object for join_scan from takatori objects
     * @param kind the kind of the join
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param storage_name the storage name to find
     * @param columns takatori join_find column information
     * @param keys takatori join_find key information
     * @param condition additional join condition
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    index_join(
        join_kind kind,
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        yugawara::storage::index const& primary_idx,
        sequence_view<takatori::relation::join_scan::column const> columns,
        takatori::tree::tree_fragment_vector<takatori::relation::join_scan::key> const& begin_for_scan,
        kvs::end_point_kind begin_endpoint,
        takatori::tree::tree_fragment_vector<takatori::relation::join_scan::key> const& end_for_scan,
        kvs::end_point_kind end_endpoint,
        takatori::util::optional_ptr<takatori::scalar::expression const> condition,
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
     * @details process record, join variables with found result, and invoke downstream when join conditions are met
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     * @return status of the operation
     */
    operation_status operator()(index_join_context& ctx, abstract::task_context* context = nullptr);

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @brief return storage name
     * @return the storage name of the find target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept;

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context*) override;

    /**
     * @brief accessor to key columns
     */
    [[nodiscard]] std::vector<index::field_info> const& key_columns() const noexcept;

    /**
     * @brief accessor to value columns
     */
    [[nodiscard]] std::vector<index::field_info> const& value_columns() const noexcept;

    /**
     * @brief accessor to key fields
     */
    [[nodiscard]] std::vector<details::search_key_field_info> const& search_key_fields() const noexcept;

private:
    join_kind join_kind_{};
    bool for_join_scan_{};
    bool use_secondary_{};
    std::string primary_storage_name_{};
    std::string secondary_storage_name_{};
    std::vector<index::field_info> key_columns_{};
    std::vector<index::field_info> value_columns_{};
    std::vector<details::search_key_field_info> search_key_fields_{};
    std::vector<details::search_key_field_info> begin_for_scan_{};
    kvs::end_point_kind begin_endpoint_ = kvs::end_point_kind::unbound;
    std::vector<details::search_key_field_info> end_for_scan_{};
    kvs::end_point_kind end_endpoint_ = kvs::end_point_kind::unbound;
    takatori::util::optional_ptr<takatori::scalar::expression const> condition_{};
    std::unique_ptr<operator_base> downstream_{};
    expr::evaluator evaluator_{};
    std::vector<details::secondary_index_field_info> secondary_key_fields_{};

    void nullify_output_variables(accessor::record_ref target);
};

}  // namespace jogasaki::executor::process::impl::ops
