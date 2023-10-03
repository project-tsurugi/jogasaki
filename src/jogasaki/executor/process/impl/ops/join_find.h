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

#include <takatori/relation/join_find.h>

#include <jogasaki/kvs/coder.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/index/field_info.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include "operator_base.h"
#include "join_find_context.h"
#include "index_field_mapper.h"
#include "details/search_key_field_info.h"

namespace jogasaki::executor::process::impl::ops {

namespace details {

/**
 * @brief matcher object to encapsulate difference between single result find and multiple result
 */
class matcher {
public:
    using memory_resource = memory::lifo_paged_memory_resource;

    matcher(
        bool use_secondary,
        std::vector<details::search_key_field_info> const& key_fields,
        std::vector<index::field_info> key_columns,
        std::vector<index::field_info> value_columns
    );

    /**
     * @brief execute the matching
     * @return true if match is successful
     * @return false if match is not successful, check status with result() function to see if the result is
     * simply not-found or other error happened.
     */
    [[nodiscard]] bool operator()(
        variable_table& input_variables,
        variable_table& output_variables,
        kvs::storage& primary_stg,
        kvs::storage* secondary_stg,
        transaction_context& tx,
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
    std::vector<details::search_key_field_info> const& key_fields_;
    data::aligned_buffer buf_{};
    status status_{status::ok};
    index_field_mapper field_mapper_{};

    variable_table* output_variables_{};
    kvs::storage* primary_storage_{};
    transaction_context* tx_{};
    matcher::memory_resource* resource_{};
    std::unique_ptr<kvs::iterator> it_{};
};

}

/**
 * @brief join_find operator
 */
class join_find : public record_operator {
public:
    friend class join_find_context;

    using column = takatori::relation::join_find::column;
    using key = takatori::relation::join_find::key;

    using memory_resource = memory::lifo_paged_memory_resource;
    /**
     * @brief create empty object
     */
    join_find() = default;

    /**
     * @brief create new object
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
    join_find(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::string_view primary_storage_name,
        std::string_view secondary_storage_name,
        std::vector<index::field_info> key_columns,
        std::vector<index::field_info> value_columns,
        std::vector<details::search_key_field_info> search_key_fields,
        takatori::util::optional_ptr<takatori::scalar::expression const> condition,
        std::unique_ptr<operator_base> downstream = nullptr,
        variable_table_info const* input_variable_info = nullptr,
        variable_table_info const* output_variable_info = nullptr
    ) noexcept;

    /**
     * @brief create new object from takatori columns
     * @param index the index to identify the operator in the process
     * @param info processor's information where this operation is contained
     * @param block_index the index of the block that this operation belongs to
     * @param storage_name the storage name to find
     * @param columns takatori join_find column information
     * @param keys takatori join_find key information
     * @param condition additional join condition
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    join_find(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        yugawara::storage::index const& primary_idx,
        sequence_view<column const> columns,
        takatori::tree::tree_fragment_vector<key> const& keys,
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
    operation_status operator()(join_find_context& ctx, abstract::task_context* context = nullptr);

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
    bool use_secondary_{};
    std::string primary_storage_name_{};
    std::string secondary_storage_name_{};
    std::vector<index::field_info> key_columns_{};
    std::vector<index::field_info> value_columns_{};
    std::vector<details::search_key_field_info> search_key_fields_{};
    takatori::util::optional_ptr<takatori::scalar::expression const> condition_{};
    std::unique_ptr<operator_base> downstream_{};
    expression::evaluator evaluator_{};

};


}


