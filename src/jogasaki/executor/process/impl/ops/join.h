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
#include <glog/logging.h>

#include <takatori/relation/step/join.h>
#include <takatori/util/downcast.h>

#include <jogasaki/data/any.h>
#include <jogasaki/model/task.h>
#include <jogasaki/model/step.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/process/impl/expression/evaluator.h>
#include <jogasaki/executor/process/impl/ops/details/expression_error.h>
#include <jogasaki/executor/process/impl/expression/evaluator_context.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/utils/iterator_pair.h>
#include <jogasaki/utils/iterator_incrementer.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include "join_context.h"
#include "context_helper.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

template <class Iterator>
class join : public cogroup_operator<Iterator> {
public:
    friend class join_context;

    using input_index = std::size_t;

    using join_kind = takatori::relation::join_kind;

    using iterator = Iterator;
    using iterator_pair = utils::iterator_pair<iterator>;
    using iterator_incrementer = utils::iterator_incrementer<iterator>;

    // parent is class template, so you should tell compiler the names in ancestors
    using operator_base::index;
    using operator_base::block_index;

    join() = default;

    join(
        operator_base::operator_index_type index,
        processor_info const& info,
        operator_base::block_index_type block_index,
        join_kind kind,
        takatori::util::optional_ptr<takatori::scalar::expression const> expression,  //NOLINT
        std::unique_ptr<operator_base> downstream = nullptr
    ) : cogroup_operator<Iterator>(index, info, block_index),
        kind_(kind),
        evaluator_(create_evaluator(expression, info.compiled_info(), info.host_variables())),
        has_condition_(expression.has_value()),
        downstream_(std::move(downstream))
    {}

    /**
     * @brief create context (if needed) and process cogroup
     * @param context task-wide context used to create operator context
     * @param cgrp the cogroup to process
     * @return status of the operation
     */
    operation_status process_cogroup(abstract::task_context* context, cogroup<iterator>& cgrp) override {
        BOOST_ASSERT(context != nullptr);  //NOLINT
        context_helper ctx{*context};
        auto* p = find_context<join_context>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<join_context>(
                index(),
                ctx.variable_table(block_index()),
                ctx.resource(),
                ctx.varlen_resource()
            );
        }
        return (*this)(*p, cgrp, context);
    }

    bool groups_available(cogroup<iterator>& cgrp, bool except_primary) {
        for(std::size_t i=(except_primary ? 1 : 0), n=cgrp.groups().size();i < n; ++i) {
            auto&& g = cgrp.groups()[i];
            if (g.empty()) {
                return false;
            }
        }
        return true;
    }

    void assign_values(
        join_context& ctx,
        cogroup<iterator>& cgrp,
        iterator_incrementer& incr,
        bool force_nulls_except_primary
    ) {
        auto target = ctx.output_variables().store().ref();
        auto& cur = incr.current();
        for(std::size_t i=0, n=cgrp.groups().size(); i < n; ++i) {
            auto&& g = cgrp.groups()[i];
            for(auto&& f : g.fields()) {

                if(empty(cur[i]) || (force_nulls_except_primary && i != 0)) {
                    target.set_null(f.target_nullity_offset_, true);
                    continue;
                }
                auto it = cur[i].first;
                auto src = f.is_key_ ? g.key() : *it;
                utils::copy_nullable_field(
                    f.type_,
                    target,
                    f.target_offset_,
                    f.target_nullity_offset_,
                    src,
                    f.source_offset_,
                    f.source_nullity_offset_,
                    ctx.varlen_resource()
                ); // TODO no need to copy between resources
            }
        }
    }

    data::any assign_and_evaluate_condition(
        join_context& ctx,
        cogroup<iterator>& cgrp,
        iterator_incrementer& incr
    ) {
        assign_values(ctx, cgrp, incr, false);
        auto resource = ctx.varlen_resource();
        auto& vars = ctx.input_variables();
        expression::evaluator_context c{};
        if(!has_condition_) {
            return data::any{std::in_place_type<bool>, true};
        }
        return evaluate_bool(c, evaluator_, vars, resource);
    }

    bool call_downstream(
        abstract::task_context* context
    ) {
        if (downstream_) {
            if(auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); !st) {
                return false;
            }
        }
        return true;
    }

    /**
     * @brief process record with context object
     * @param ctx operator context object for the execution
     * @return status of the operation
     */
    operation_status operator()(  //NOLINT(readability-function-cognitive-complexity)
        join_context& ctx,
        cogroup<iterator>& cgrp,
        abstract::task_context* context = nullptr
    ) {
        constexpr static std::size_t primary_group_index = 0;
        constexpr static std::size_t secondary_group_index = 1;

        if (ctx.inactive()) {
            return {operation_status_kind::aborted};
        }
        std::size_t n = cgrp.groups().size();
        std::vector<iterator_pair> iterators{};
        iterators.reserve(n);
        for(auto&& g : cgrp.groups()) {
            iterators.emplace_back(g.begin(), g.end());
        }
        BOOST_ASSERT(kind_ == join_kind::inner || kind_ == join_kind::full_outer || n == 2); //NOLINT
        BOOST_ASSERT(! (has_condition_ && kind_ == join_kind::full_outer)); //NOLINT
        iterator_incrementer incr{std::move(iterators)};
        switch(kind_) {
            case join_kind::full_outer: //fall-thru
            case join_kind::inner: {
                if(kind_ == join_kind::inner && ! groups_available(cgrp, false)) {
                    break;
                }
                do {
                    auto a = assign_and_evaluate_condition(ctx, cgrp, incr);
                    if(a.error()) {
                        return handle_expression_error(ctx, a);
                    }
                    if(a.template to<bool>()) {
                        if(! call_downstream(context)) {
                            ctx.abort();
                            return {operation_status_kind::aborted};
                        }
                    }
                } while (incr.increment());
                break;
            }
            case join_kind::left_outer: {
                if(cgrp.groups()[0].empty()) {
                    break;
                }
                bool secondary_group_available = groups_available(cgrp, true);
                do {
                    bool exists_match = false;
                    if(secondary_group_available) {
                        do {
                            auto a = assign_and_evaluate_condition(ctx, cgrp, incr);
                            if (a.error()) {
                                return handle_expression_error(ctx, a);
                            }
                            if (a.template to<bool>()) {
                                exists_match = true;
                                if (!call_downstream(context)) {
                                    ctx.abort();
                                    return {operation_status_kind::aborted};
                                }
                            }
                        } while (incr.increment(secondary_group_index));
                        incr.reset(secondary_group_index);
                    }
                    if(! exists_match) {
                        // assign nulls for non-primary groups
                        assign_values(ctx, cgrp, incr, true);
                        if (! call_downstream(context)) {
                            ctx.abort();
                            return {operation_status_kind::aborted};
                        }
                    }
                } while (incr.increment(primary_group_index));
                break;
            }
            case join_kind::anti: //fall-thru
            case join_kind::semi: {
                if(cgrp.groups()[0].empty()) {
                    break;
                }
                do {
                    bool exists_match = false;
                    if(groups_available(cgrp, true)) {
                        do {
                            auto a = assign_and_evaluate_condition(ctx, cgrp, incr);
                            if (a.error()) {
                                return handle_expression_error(ctx, a);
                            }
                            if (a.template to<bool>()) {
                                exists_match = true;
                                break;
                            }
                        } while (incr.increment(secondary_group_index));
                        incr.reset(secondary_group_index);
                    }
                    if((exists_match && kind_ == join_kind::semi) || (!exists_match && kind_ == join_kind::semi)) {
                        if (! call_downstream(context)) {
                            ctx.abort();
                            return {operation_status_kind::aborted};
                        }
                    }
                } while (incr.increment(primary_group_index));
                break;
            }
        }
        return {};
    }

    /**
     * @see operator_base::kind()
     */
    [[nodiscard]] operator_kind kind() const noexcept override {
        return operator_kind::join;
    }

    /**
     * @see operator_base::finish()
     */
    void finish(abstract::task_context* context) override {
        if (! context) return;
        context_helper ctx{*context};
        if (auto* p = find_context<join_context>(index(), ctx.contexts())) {
            p->release();
        }
        if (downstream_) {
            unsafe_downcast<record_operator>(downstream_.get())->finish(context);
        }
    }

private:
    join_kind kind_{};
    expression::evaluator evaluator_{};
    bool has_condition_{};
    std::unique_ptr<operator_base> downstream_{};

    expression::evaluator create_evaluator(
        takatori::util::optional_ptr<takatori::scalar::expression const> expression,  //NOLINT
        yugawara::compiled_info const& compiled_info,
        variable_table const* host_variables
    ) {
        if (expression) {
            return expression::evaluator(*expression, compiled_info, host_variables);
        }
        return {};
    }
};

}