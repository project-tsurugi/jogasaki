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

#include <takatori/relation/join_find.h>

#include <jogasaki/kvs/coder.h>
#include <jogasaki/data/aligned_buffer.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include "operator_base.h"
#include "join_find_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::maybe_shared_ptr;
using takatori::util::unsafe_downcast;

namespace details {

/**
 * @brief field info of the join_find operation
 * @details join_find operator uses these fields to know how the found key/value are mapped to scope variables
 */
struct cache_align join_find_column {
    /**
     * @brief create new join_find column
     * @param type type of the target column
     * @param target_exists whether the target storage exists. If not, there is no room to copy the data to.
     * @param offset byte offset of the target field in the target record reference (scope variable)
     * @param nullity_offset bit offset of the target field nullity in the target record reference (scope variable)
     * @param nullable whether the source field is nullable or not
     * @param spec the spec of the target field used for encode/decode
     */
    join_find_column(
        meta::field_type type,
        bool target_exists,
        std::size_t offset,
        std::size_t nullity_offset,
        bool nullable,
        kvs::coding_spec spec
    );

    meta::field_type type_{}; //NOLINT
    bool target_exists_{}; //NOLINT
    std::size_t offset_{}; //NOLINT
    std::size_t nullity_offset_{}; //NOLINT
    bool nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
};

/**
 * @brief key field info of the join_find operation
 * @details join_find operator uses these fields to know how to create search key sequence from the  scope variables
 */
struct cache_align join_find_key_field {
    /**
     * @brief create new join key field
     * @param type type of the key field
     * @param nullable whether the target field is nullable or not
     * @param spec the spec of the target field used for encode/decode
     * @param evaluator evaluator used to evaluate the key field value
     */
    join_find_key_field(
        meta::field_type type,
        bool nullable,
        kvs::coding_spec spec,
        expression::evaluator evaluator
    );

    meta::field_type type_{}; //NOLINT
    bool nullable_{}; //NOLINT
    kvs::coding_spec spec_{}; //NOLINT
    expression::evaluator evaluator_{}; //NOLINT
};

/**
 * @brief matcher object to encapsulate difference between single result find and multiple result
 */
class matcher {
public:
    using memory_resource = memory::lifo_paged_memory_resource;

    matcher() = default;

    matcher(
        std::vector<details::join_find_key_field> const& key_fields,
        std::vector<details::join_find_column> const& key_columns,
        std::vector<details::join_find_column> const& value_columns
    );

    void read_stream(
        executor::process::impl::block_scope& scope,
        memory_resource* resource,
        kvs::stream& src,
        std::vector<details::join_find_column> const& columns
    );

    [[nodiscard]] bool operator()(
        executor::process::impl::block_scope& scope,
        kvs::storage& stg,
        kvs::transaction& tx,
        memory_resource* resource = nullptr
    );

    bool next();

private:
    std::vector<details::join_find_key_field> const& key_fields_{};
    std::vector<details::join_find_column> const& key_columns_{};
    std::vector<details::join_find_column> const& value_columns_{};
    data::aligned_buffer buf_{};
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
     * @param storage_name the storage name to find
     * @param key_columns column information for key fields
     * @param value_columns column information for value fields
     * @param key_fields key_field information
     * @param condition additional join condition
     * @param downstream downstream operator invoked after this operation. Pass nullptr if such dispatch is not needed.
     */
    join_find(
        operator_index_type index,
        processor_info const& info,
        block_index_type block_index,
        std::string_view storage_name,
        std::vector<details::join_find_column> key_columns,
        std::vector<details::join_find_column> value_columns,
        std::vector<details::join_find_key_field> key_fields,
        takatori::util::optional_ptr<takatori::scalar::expression const> condition,
        std::unique_ptr<operator_base> downstream = nullptr
    ) noexcept :
        record_operator(index, info, block_index),
        storage_name_(storage_name),
        key_columns_(std::move(key_columns)),
        value_columns_(std::move(value_columns)),
        key_fields_(std::move(key_fields)),
        condition_(std::move(condition)),
        downstream_(std::move(downstream)),
        evaluator_(condition_ ? expression::evaluator{*condition_, info.compiled_info()} : expression::evaluator{})
    {}

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
        std::string_view storage_name,
        yugawara::storage::index const& idx,
        sequence_view<column const> columns,
        takatori::tree::tree_fragment_vector<key> const& keys,
        takatori::util::optional_ptr<takatori::scalar::expression const> condition,
        std::unique_ptr<operator_base> downstream
    );

    /**
     * @brief create context (if needed) and process record
     * @param context task-wide context used to create operator context
     */
    void process_record(abstract::task_context* context) override;

    /**
     * @brief process record with context object
     * @details process record, join variables with found result, and invoke downstream when join conditions are met
     * @param ctx operator context object for the execution
     * @param context task context for the downstream, can be nullptr if downstream doesn't require.
     */
    void operator()(join_find_context& ctx, abstract::task_context* context = nullptr);

    [[nodiscard]] operator_kind kind() const noexcept override;

    /**
     * @brief return storage name
     * @return the storage name of the find target
     */
    [[nodiscard]] std::string_view storage_name() const noexcept;

    void finish(abstract::task_context*) override;

    /**
     * @brief accessor to key columns
     */
    [[nodiscard]] std::vector<details::join_find_column> const& key_columns() const noexcept;

    /**
     * @brief accessor to value columns
     */
    [[nodiscard]] std::vector<details::join_find_column> const& value_columns() const noexcept;

    /**
     * @brief accessor to key fields
     */
    [[nodiscard]] std::vector<details::join_find_key_field> const& key_fields() const noexcept;

private:
    std::string storage_name_{};
    std::vector<details::join_find_column> key_columns_{};
    std::vector<details::join_find_column> value_columns_{};
    std::vector<details::join_find_key_field> key_fields_{};
    takatori::util::optional_ptr<takatori::scalar::expression const> condition_{};
    std::unique_ptr<operator_base> downstream_{};
    expression::evaluator evaluator_{};

    std::vector<details::join_find_column> create_columns(
        yugawara::storage::index const& idx,
        sequence_view<column const> columns,
        processor_info const& info,
        block_index_type block_index,
        bool key
    );

    std::vector<details::join_find_key_field> create_key_fields(
        yugawara::storage::index const& idx,
        takatori::tree::tree_fragment_vector<key> const& keys,
        processor_info const& info
    );
};


}


