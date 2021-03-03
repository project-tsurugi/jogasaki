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

#include <takatori/descriptor/variable.h>
#include <takatori/relation/step/aggregate.h>
#include <takatori/util/downcast.h>
#include <takatori/util/sequence_view.h>
#include <yugawara/binding/extract.h>

#include <jogasaki/executor/function/aggregate_function_info.h>
#include <jogasaki/executor/function/aggregate_function_repository.h>
#include <jogasaki/utils/field_types.h>
#include "operator_base.h"
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
    ) :
        type_(std::move(type)),
        argument_indices_(std::move(argument_indices)),
        function_info_(std::move(function_info)),
        offset_(offset),
        nullity_offset_(nullity_offset),
        nullable_(nullable)
    {}

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
    ) noexcept :
        type_(std::move(type)),
        offset_(offset),
        nullity_offset_(nullity_offset),
        nullable_(nullable)
    {}

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
    ) {
        auto var_indices = variable_indices(columns).second;
        std::vector<details::aggregate_group_column> ret{};
        for(auto&& c : columns) {
            std::vector<std::size_t> argument_indices{};
            for(auto&& a : c.arguments()) {
                argument_indices.emplace_back(var_indices[a]);
            }
            auto& decl = yugawara::binding::extract<yugawara::aggregate::declaration>(c.function());
            auto& repo = global::aggregate_function_repository();
            auto f = repo.find(decl.definition_id());
            BOOST_ASSERT(f != nullptr);  //NOLINT
            auto& v = this->block_info().value_map().at(c.destination());
            ret.emplace_back(
                utils::type_for(compiled_info().type_of(c.destination())),
                std::move(argument_indices),
                *f,
                v.value_offset(),
                v.nullity_offset(),
                true  // currently scope variables are all nullable
            );
        }
        return ret;
    }

    std::vector<details::aggregate_group_argument> create_arguments(
        sequence_view<column const> columns
    ) {
        auto vars = variable_indices(columns).first;
        std::vector<details::aggregate_group_argument> ret{};
        ret.reserve(vars.size());
        for(auto&& v : vars) {
            ret.emplace_back(
                utils::type_for(compiled_info().type_of(v)),
                block_info().value_map().at(v).value_offset(),
                block_info().value_map().at(v).nullity_offset(),
                true
            );
        }
        return ret;
    }

    std::pair<
        std::vector<takatori::descriptor::variable>,
        std::unordered_map<takatori::descriptor::variable, std::size_t>
    >
    variable_indices(sequence_view<column const> columns) {
        std::size_t index = 0;
        std::vector<takatori::descriptor::variable> first{};
        std::unordered_map<takatori::descriptor::variable, std::size_t> second{};
        first.reserve(columns.size());
        for(auto&& c : columns) {
            for(auto&& a : c.arguments()) {
                if (second.count(a) == 0) {
                    second[a] = index;
                    ++index;
                    first.emplace_back(a);
                }
            }
        }
        return {std::move(first), std::move(second)};
    }
};

}


