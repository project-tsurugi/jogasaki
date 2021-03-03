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

#include "operator_base.h"
#include "context_helper.h"
#include "aggregate_group_context.h"

#include <jogasaki/executor/function/field_locator.h>

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

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

operation_status aggregate_group::process_group(abstract::task_context* context, bool last_member) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<aggregate_group_context>(index(), ctx.contexts());
    if (! p) {
        std::vector<data::value_store> stores{};
        std::vector<std::unique_ptr<memory::lifo_paged_memory_resource>> resources{};
        stores.reserve(arguments_.size());
        resources.reserve(arguments_.size());
        for(auto&& a : arguments_) {
            auto& res = resources.emplace_back(std::make_unique<memory::lifo_paged_memory_resource>(&global::page_pool()));
            stores.emplace_back(
                a.type_,
                res.get(),
                ctx.varlen_resource()
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
            ctx.block_scope(block_index()),
            ctx.resource(),
            ctx.varlen_resource(),
            std::move(stores),
            std::move(resources),
            std::move(function_arg_stores)
        );
    }
    return (*this)(*p, last_member, context);
}

template <meta::field_type_kind Kind>
using rtype = typename meta::field_type_traits<Kind>::runtime_type;

void copy_value(
    accessor::record_ref src,
    std::size_t offset,
    std::size_t nullity_offset,
    bool nullable,
    data::value_store& dest
) {
    using kind = meta::field_type_kind;
    // TODO handle nulls
    (void)nullity_offset;
    (void)nullable;
    switch(dest.type().kind()) {
        case kind::int4: dest.append(src.get_value<rtype<kind::int4>>(offset)); break;
        case kind::int8: dest.append(src.get_value<rtype<kind::int8>>(offset)); break;
        case kind::float4: dest.append(src.get_value<rtype<kind::float4>>(offset)); break;
        case kind::float8: dest.append(src.get_value<rtype<kind::float8>>(offset)); break;
        case kind::character: dest.append(src.get_value<rtype<kind::character>>(offset)); break;
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
        auto src = ctx.variables().store().ref();
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
            auto target = ctx.variables().store().ref();
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
        }
    }
    return {};
}

operator_kind aggregate_group::kind() const noexcept {
    return operator_kind::aggregate_group;
}

void aggregate_group::finish(abstract::task_context* context) {
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

}


