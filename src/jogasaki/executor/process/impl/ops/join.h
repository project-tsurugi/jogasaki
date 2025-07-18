/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include <utility>
#include <vector>
#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/relation/join_kind.h>
#include <takatori/relation/step/join.h>
#include <takatori/scalar/expression.h>
#include <takatori/util/downcast.h>
#include <takatori/util/infect_qualifier.h>
#include <takatori/util/optional_ptr.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/accessor/record_ref.h>
#include <jogasaki/data/any.h>
#include <jogasaki/data/iterable_record_store.h>
#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/abstract/task_context.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/cogroup.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/details/expression_error.h>
#include <jogasaki/executor/process/impl/ops/operation_status.h>
#include <jogasaki/executor/process/impl/ops/operator_base.h>
#include <jogasaki/executor/process/impl/ops/operator_kind.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/processor_info.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/meta/group_meta.h>
#include <jogasaki/model/step.h>
#include <jogasaki/model/task.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/checkpoint_holder.h>
#include <jogasaki/utils/copy_field_data.h>
#include <jogasaki/utils/iterator_incrementer.h>
#include <jogasaki/utils/iterator_pair.h>

#include "context_helper.h"
#include "join_context.h"

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
        bool force_nulls_except_primary,
        bool force_nulls_on_primary = false
    ) {
        auto target = ctx.output_variables().store().ref();
        auto& cur = incr.current();
        for(std::size_t i=0, n=cgrp.groups().size(); i < n; ++i) {
            auto&& g = cgrp.groups()[i];
            for(auto&& f : g.fields()) {
                if(empty(cur[i]) || (force_nulls_except_primary && i != 0) || (force_nulls_on_primary && i == 0)) {
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
        iterator_incrementer& incr,
        expr::evaluator_context& context
    ) {
        assign_values(ctx, cgrp, incr, false);
        auto resource = ctx.varlen_resource();
        auto& vars = ctx.input_variables();
        if(!has_condition_) {
            return data::any{std::in_place_type<bool>, true};
        }
        return evaluate_bool(context, evaluator_, vars, resource);
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
        assert_with_exception(kind_ == join_kind::inner || kind_ == join_kind::full_outer || n == 2, kind_, n); //NOLINT
        assert_with_exception(! (has_condition_ && kind_ == join_kind::full_outer && n >= 3), has_condition_, kind_, n); //NOLINT
        iterator_incrementer incr{std::move(iterators)};
        switch(kind_) {
            case join_kind::inner: {
                if(kind_ == join_kind::inner && ! groups_available(cgrp, false)) {
                    break;
                }
                do {
                    expr::evaluator_context c {
                        ctx.varlen_resource(),
                        ctx.req_context() ? ctx.req_context()->transaction().get() : nullptr
                    };
                    auto a = assign_and_evaluate_condition(ctx, cgrp, incr, c);
                    if(a.error()) {
                        return handle_expression_error(ctx, a, c);
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
                            expr::evaluator_context c{
                                ctx.varlen_resource(),
                                ctx.req_context() ? ctx.req_context()->transaction().get() : nullptr
                            };
                            auto a = assign_and_evaluate_condition(ctx, cgrp, incr, c);
                            if (a.error()) {
                                return handle_expression_error(ctx, a, c);
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
            case join_kind::full_outer: {
                assert_with_exception(n == 2, n); //NOLINT
                // for now, we assume full outer join has only two groups
                bool secondary_group_available = groups_available(cgrp, true);
                auto right_group_size = secondary_group_available ? cgrp.groups()[1].size() : 0;
                boost::dynamic_bitset<std::uint64_t> unmatched_right{right_group_size};
                unmatched_right.flip(); // initially all right records are unmatched
                do {
                    bool exists_match = false;
                    if(secondary_group_available) {
                        std::size_t secondary_group_pos = 0;
                        do {
                            expr::evaluator_context c{
                                ctx.varlen_resource(),
                                ctx.req_context() ? ctx.req_context()->transaction().get() : nullptr
                            };
                            auto a = assign_and_evaluate_condition(ctx, cgrp, incr, c);
                            if (a.error()) {
                                return handle_expression_error(ctx, a, c);
                            }
                            if (a.template to<bool>()) {
                                exists_match = true;
                                unmatched_right.reset(secondary_group_pos);
                                if (!call_downstream(context)) {
                                    ctx.abort();
                                    return {operation_status_kind::aborted};
                                }
                            }
                            ++secondary_group_pos;
                        } while (incr.increment(secondary_group_index));
                        incr.reset(secondary_group_index);
                    }
                    if(! exists_match && ! cgrp.groups()[0].empty()) {
                        // left exists and it does not have a match
                        // assign nulls for non-primary groups
                        assign_values(ctx, cgrp, incr, true);
                        if (! call_downstream(context)) {
                            ctx.abort();
                            return {operation_status_kind::aborted};
                        }
                    }
                } while (incr.increment(primary_group_index));

                incr.reset();
                for(std::size_t i=0; i < right_group_size; ++i) {
                    if(unmatched_right.test(i)) {
                        // assign nulls for primary group
                        assign_values(ctx, cgrp, incr, false, true);
                        if (! call_downstream(context)) {
                            ctx.abort();
                            return {operation_status_kind::aborted};
                        }
                    }
                    (void) incr.increment(secondary_group_index);
                }
                break;
            }
            case join_kind::anti: [[fallthrough]];
            case join_kind::semi: {
                if(cgrp.groups()[0].empty()) {
                    break;
                }
                do {
                    bool exists_match = false;
                    if(groups_available(cgrp, true)) {
                        do {
                            expr::evaluator_context c{
                                ctx.varlen_resource(),
                                ctx.req_context() ? ctx.req_context()->transaction().get() : nullptr
                            };
                            auto a = assign_and_evaluate_condition(ctx, cgrp, incr, c);
                            if (a.error()) {
                                return handle_expression_error(ctx, a, c);
                            }
                            if (a.template to<bool>()) {
                                exists_match = true;
                                break;
                            }
                        } while (incr.increment(secondary_group_index));
                        incr.reset(secondary_group_index);
                    }
                    if((exists_match && kind_ == join_kind::semi) || (! exists_match && kind_ == join_kind::anti)) {
                        assign_values(ctx, cgrp, incr, true);
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
    expr::evaluator evaluator_{};
    bool has_condition_{};
    std::unique_ptr<operator_base> downstream_{};

    expr::evaluator create_evaluator(
        takatori::util::optional_ptr<takatori::scalar::expression const> expression,  //NOLINT
        yugawara::compiled_info const& compiled_info,
        variable_table const* host_variables
    ) {
        if (expression) {
            return expr::evaluator(*expression, compiled_info, host_variables);
        }
        return {};
    }
};

}
