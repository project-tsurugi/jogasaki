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
#include "aggregate_group.h"

#include <takatori/util/fail.h>

#include <jogasaki/executor/function/aggregate_function_repository.h>
#include <jogasaki/utils/field_types.h>
#include "context_helper.h"
#include "aggregate_group_context.h"

#include <jogasaki/executor/function/field_locator.h>

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;
using takatori::util::fail;

aggregate_group::aggregate_group(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    sequence_view<column const> columns,
    std::unique_ptr<operator_base> downstream
) :
    group_operator(index, info, block_index),
    downstream_(std::move(downstream)),
    columns_(create_columns(columns)),
    arguments_(create_arguments(columns))
{}

aggregate_group_context* aggregate_group::create_context_if_not_found(abstract::task_context* context) {
    context_helper ctx{*context};
    auto* p = find_context<aggregate_group_context>(index(), ctx.contexts());
    if (! p) {
        std::vector<data::value_store> stores{};
        std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> resources{};
        std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> nulls_resources{};
        stores.reserve(arguments_.size());
        resources.reserve(arguments_.size());
        nulls_resources.reserve(arguments_.size());
        for(auto&& a : arguments_) {
            auto& res = resources.emplace_back(
                std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool())
            );
            auto& nulls_res = nulls_resources.emplace_back(
                std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool())
            );
            stores.emplace_back(
                a.type_,
                res.get(),
                ctx.varlen_resource(),
                nulls_res.get()
            );
        }

        std::vector<std::vector<std::reference_wrapper<data::value_store>>> function_arg_stores{};
        function_arg_stores.reserve(columns_.size());
        for(auto&& c : columns_) {
            auto& store_refs = function_arg_stores.emplace_back();
            store_refs.reserve(c.argument_indices_.size());
            for(auto idx : c.argument_indices_) {
                store_refs.emplace_back(stores[idx]);
            }
        }
        p = ctx.make_context<aggregate_group_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource(),
            std::move(stores),
            std::move(resources),
            std::move(function_arg_stores),
            std::move(nulls_resources)
        );
    }
    return p;
}

operation_status aggregate_group::process_group(abstract::task_context* context, bool last_member) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    auto p = create_context_if_not_found(context);
    return (*this)(*p, last_member, context);
}

void copy_value(
    accessor::record_ref src,
    std::size_t offset,
    std::size_t nullity_offset,
    bool nullable,
    data::value_store& dest
) {
    using kind = meta::field_type_kind;
    if (nullable && src.is_null(nullity_offset)) {
        dest.append_null();
        return;
    }
    switch(dest.type().kind()) {
        case kind::int4: dest.append(src.get_value<runtime_t<kind::int4>>(offset)); break;
        case kind::int8: dest.append(src.get_value<runtime_t<kind::int8>>(offset)); break;
        case kind::float4: dest.append(src.get_value<runtime_t<kind::float4>>(offset)); break;
        case kind::float8: dest.append(src.get_value<runtime_t<kind::float8>>(offset)); break;
        case kind::character: dest.append(src.get_value<runtime_t<kind::character>>(offset)); break;
        case kind::octet: dest.append(src.get_value<runtime_t<kind::octet>>(offset)); break;
        case kind::decimal: dest.append(src.get_value<runtime_t<kind::decimal>>(offset)); break;
        case kind::date: dest.append(src.get_value<runtime_t<kind::date>>(offset)); break;
        case kind::time_of_day: dest.append(src.get_value<runtime_t<kind::time_of_day>>(offset)); break;
        case kind::time_point: dest.append(src.get_value<runtime_t<kind::time_point>>(offset)); break;
        default: fail();
    }
}

operation_status aggregate_group::operator()(
    aggregate_group_context& ctx,
    bool last_member,
    abstract::task_context* context
) {
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }
    for(std::size_t i=0, n=arguments_.size(); i < n; ++i) {
        // append value store the values
        auto& store = ctx.stores_[i];
        auto& arg = arguments_[i];
        auto src = ctx.input_variables().store().ref();
        copy_value(
            src,
            arg.offset_,
            arg.nullity_offset_,
            arg.nullable_,
            store
        );
    }

    if (last_member) {
        // do aggregation from value store and create column values
        for(std::size_t i=0, n=columns_.size(); i < n; ++i) {
            auto& c = columns_[i];
            auto& func = c.function_info_.aggregator();
            auto target = ctx.output_variables().store().ref();
            func(target,
                function::field_locator{
                    c.type_,
                    c.nullable_,
                    c.offset_,
                    c.nullity_offset_
                },
                ctx.function_arg_stores_[i]
            );
        }

        if (downstream_) {
            if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
                ctx.abort();
                return {operation_status_kind::aborted};
            }
        }
        // reset
        for(std::size_t i=0, n=columns_.size(); i < n; ++i) {
            ctx.stores_[i].reset();
            ctx.resources_[i]->deallocate_after(memory::lifo_paged_memory_resource::initial_checkpoint);
            ctx.nulls_resources_[i]->deallocate_after(memory::lifo_paged_memory_resource::initial_checkpoint);
        }
    }
    return {};
}

operator_kind aggregate_group::kind() const noexcept {
    return operator_kind::aggregate_group;
}

void aggregate_group::finish(abstract::task_context* context) {
    auto& ctx = *create_context_if_not_found(context);
    context_helper helper{*context};
    if (ctx.inactive()) {
        return;
    }
    if (helper.empty_input_from_shuffle()) {
        // do aggregation from value store and create column values
        for(auto & c : columns_) {
            auto& func = c.function_info_.empty_value_generator();
            auto target = ctx.output_variables().store().ref();
            func(target,
                function::field_locator{
                    c.type_,
                    c.nullable_,
                    c.offset_,
                    c.nullity_offset_
                }
            );
        }

        if (downstream_) {
            if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
                ctx.abort();
            }
        }
    }
    ctx.release();
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

std::vector<details::aggregate_group_column> aggregate_group::create_columns(sequence_view<column const> columns) {
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
        auto& v = this->block_info().at(c.destination());
        ret.emplace_back(
            utils::type_for(compiled_info().type_of(c.destination())),
            std::move(argument_indices),
            *f,
            v.value_offset(),
            v.nullity_offset(),
            true  // currently variables are all nullable
        );
    }
    return ret;
}

std::vector<details::aggregate_group_argument> aggregate_group::create_arguments(sequence_view<column const> columns) {
    auto vars = variable_indices(columns).first;
    std::vector<details::aggregate_group_argument> ret{};
    ret.reserve(vars.size());
    for(auto&& v : vars) {
        ret.emplace_back(
            utils::type_for(compiled_info().type_of(v)),
            block_info().at(v).value_offset(),
            block_info().at(v).nullity_offset(),
            true
        );
    }
    return ret;
}

std::pair<
    std::vector<takatori::descriptor::variable>,
    std::unordered_map<takatori::descriptor::variable, std::size_t>
>
aggregate_group::variable_indices(sequence_view<column const> columns) {
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

details::aggregate_group_column::aggregate_group_column(
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


details::aggregate_group_argument::aggregate_group_argument(
    meta::field_type type,
    std::size_t offset,
    std::size_t nullity_offset,
    bool nullable
) noexcept:
    type_(std::move(type)),
    offset_(offset),
    nullity_offset_(nullity_offset),
    nullable_(nullable)
{}

}


