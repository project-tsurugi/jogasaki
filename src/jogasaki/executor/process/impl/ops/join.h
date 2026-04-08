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
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/comparator.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/global.h>
#include <jogasaki/executor/io/group_reader.h>
#include <jogasaki/executor/io/reader_container.h>
#include <jogasaki/executor/process/abstract/task_context.h>
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
#include <jogasaki/status.h>
#include <jogasaki/utils/assert.h>
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
        assert_with_exception(context != nullptr, context);
        context_helper ctx{*context};
        auto* p = find_context<join_context<iterator>>(index(), ctx.contexts());
        if (! p) {
            p = ctx.make_context<join_context<iterator>>(
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
        join_context<iterator>& ctx,
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
        join_context<iterator>& ctx,
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

    /**
     * @brief call downstream process_record and propagate yield or aborted status.
     * @details on yield, iteration state is not preserved in this operator, so yield from
     *          join's downstream may not resume correctly in all join kinds.
     *          TODO: save iteration state in join_context to support proper yield resume.
     * @param ctx the join context for state management
     * @param context the task context
     * @return ok, yield, or aborted
     */
    operation_status call_downstream(
        join_context<iterator>& ctx,
        abstract::task_context* context
    ) {
        if (downstream_) {
            ctx.state(context_state::calling_child);
            auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
            if (st.kind() == operation_status_kind::aborted) {
                ctx.abort();
                return operation_status_kind::aborted;
            }
            if (st.kind() == operation_status_kind::ok) {
                ctx.state(context_state::running_operator_body);
            }
            return st;
        }
        return operation_status_kind::ok;
    }

    /**
     * @brief process record with context object
     * @param ctx operator context object for the execution
     * @return status of the operation
     */
    operation_status operator()(  //NOLINT(readability-function-cognitive-complexity)
        join_context<iterator>& ctx,
        cogroup<iterator>& cgrp,
        abstract::task_context* context = nullptr
    ) {
        constexpr static std::size_t primary_group_index = 0;
        constexpr static std::size_t secondary_group_index = 1;

        assert_with_exception(ctx.state() != context_state::yielding, ctx.state());
        if (ctx.aborted()) {
            return operation_status_kind::aborted;
        }

        context_helper helper{ctx.task_context()};
        data::any result{};
        if (ctx.state() == context_state::calling_child) {
            VLOG_LP(log_trace) << "resuming join op. after downstream yield";
            switch(kind_) {
                case join_kind::inner: goto resume_calling_child_1;  //NOLINT
                case join_kind::left_outer_at_most_one: [[fallthrough]];  //NOLINT
                case join_kind::left_outer:
                    if (ctx.exists_match_) goto resume_calling_child_2;  //NOLINT
                    goto resume_calling_child_3;  //NOLINT
                case join_kind::full_outer:
                    if (ctx.resuming_calling_child_6_) goto resume_calling_child_6;  //NOLINT
                    if (ctx.exists_match_) goto resume_calling_child_4;  //NOLINT
                    goto resume_calling_child_5;  //NOLINT
                case join_kind::anti: [[fallthrough]];
                case join_kind::semi:
                    goto resume_calling_child_7;  //NOLINT
            }
        }
        {
            std::size_t n = cgrp.groups().size();
            std::vector<iterator_pair> iterators{};
            iterators.reserve(n);
            for(auto&& g : cgrp.groups()) {
                iterators.emplace_back(g.begin(), g.end());
            }
            assert_with_exception(kind_ == join_kind::inner || kind_ == join_kind::full_outer || n == 2, kind_, n);
            assert_with_exception(! (has_condition_ && kind_ == join_kind::full_outer && n >= 3), has_condition_, kind_, n);
            ctx.incr_ = iterator_incrementer{std::move(iterators)};
        }
        switch(kind_) {
            case join_kind::inner: {
                if(kind_ == join_kind::inner && ! groups_available(cgrp, false)) {
                    break;
                }
                do {
                    {
                        expr::evaluator_context c {
                            ctx.varlen_resource(),
                            ctx.req_context() ? ctx.req_context()->transaction().get() : nullptr
                        };
                        c.blob_session(std::addressof(helper.blob_session_container()));
                        result = assign_and_evaluate_condition(ctx, cgrp, ctx.incr_, c);
                        if(result.error()) {
                            handle_expression_error(*ctx.req_context(), result, c);
                            ctx.abort();
                            return operation_status_kind::aborted;
                        }
                    }
                    if(result.template to<bool>()) {
resume_calling_child_1:
                        if(auto call_st = call_downstream(ctx, context); ! call_st) {
                            return call_st;
                        }
                    }
                } while (ctx.incr_.increment());
                break;
            }
            case join_kind::left_outer_at_most_one: [[fallthrough]];
            case join_kind::left_outer: {
                if(cgrp.groups()[0].empty()) {
                    break;
                }
                ctx.secondary_group_available_ = groups_available(cgrp, true);
                do {
                    ctx.exists_match_ = false;
                    if(ctx.secondary_group_available_) {
                        do {
                            {
                                expr::evaluator_context c{
                                    ctx.varlen_resource(),
                                    ctx.req_context() ? ctx.req_context()->transaction().get() : nullptr
                                };
                                c.blob_session(std::addressof(helper.blob_session_container()));
                                result = assign_and_evaluate_condition(ctx, cgrp, ctx.incr_, c);
                                if (result.error()) {
                                    handle_expression_error(*ctx.req_context(), result, c);
                                    ctx.abort();
                                    return operation_status_kind::aborted;
                                }
                            }
                            if (result.template to<bool>()) {
                                if (kind_ == join_kind::left_outer_at_most_one && ctx.exists_match_) {
                                    // second matching row: scalar subquery returned more than one record
                                    set_error_context(
                                        *ctx.req_context(),
                                        error_code::scalar_subquery_evaluation_exception,
                                        "scalar subquery returned more than one record",
                                        status::err_expression_evaluation_failure
                                    );
                                    ctx.abort();
                                    return operation_status_kind::aborted;
                                }
                                ctx.exists_match_ = true;
resume_calling_child_2:
                                if(auto call_st = call_downstream(ctx, context); ! call_st) {
                                    return call_st;
                                }
                            }
                        } while (ctx.incr_.increment(secondary_group_index));
                        ctx.incr_.reset(secondary_group_index);
                    }
                    if(! ctx.exists_match_) {
                        // assign nulls for non-primary groups
                        assign_values(ctx, cgrp, ctx.incr_, true);
resume_calling_child_3:
                        if(auto call_st = call_downstream(ctx, context); ! call_st) {
                            return call_st;
                        }
                    }
                } while (ctx.incr_.increment(primary_group_index));
                break;
            }
            case join_kind::full_outer: {
                assert_with_exception(cgrp.groups().size() == 2, cgrp.groups().size());
                // for now, we assume full outer join has only two groups
                ctx.secondary_group_available_ = groups_available(cgrp, true);
                ctx.right_group_size_ = ctx.secondary_group_available_ ? cgrp.groups()[1].size() : 0;
                ctx.unmatched_right_ = boost::dynamic_bitset<std::uint64_t>{ctx.right_group_size_};
                ctx.unmatched_right_.flip(); // initially all right records are unmatched
                do {
                    ctx.exists_match_ = false;
                    if(ctx.secondary_group_available_) {
                        ctx.secondary_group_pos_ = 0;
                        do {
                            {
                                expr::evaluator_context c{
                                    ctx.varlen_resource(),
                                    ctx.req_context() ? ctx.req_context()->transaction().get() : nullptr
                                };
                                c.blob_session(std::addressof(helper.blob_session_container()));
                                result = assign_and_evaluate_condition(ctx, cgrp, ctx.incr_, c);
                                if (result.error()) {
                                    handle_expression_error(*ctx.req_context(), result, c);
                                    ctx.abort();
                                    return operation_status_kind::aborted;
                                }
                            }
                            if (result.template to<bool>()) {
                                ctx.exists_match_ = true;
                                ctx.unmatched_right_.reset(ctx.secondary_group_pos_);
resume_calling_child_4:
                                if(auto call_st = call_downstream(ctx, context); ! call_st) {
                                    return call_st;
                                }
                            }
                            ++ctx.secondary_group_pos_;
                        } while (ctx.incr_.increment(secondary_group_index));
                        ctx.incr_.reset(secondary_group_index);
                    }
                    if(! ctx.exists_match_ && ! cgrp.groups()[0].empty()) {
                        // left exists and it does not have a match
                        // assign nulls for non-primary groups
                        assign_values(ctx, cgrp, ctx.incr_, true);
resume_calling_child_5:
                        if(auto call_st = call_downstream(ctx, context); ! call_st) {
                            return call_st;
                        }
                    }
                } while (ctx.incr_.increment(primary_group_index));

                ctx.incr_.reset();
                for(ctx.idx_=0; ctx.idx_ < ctx.right_group_size_; ++ctx.idx_) {
                    if(ctx.unmatched_right_.test(ctx.idx_)) {
                        // assign nulls for primary group
                        assign_values(ctx, cgrp, ctx.incr_, false, true);
resume_calling_child_6:
                        ctx.resuming_calling_child_6_ = false;
                        if(auto call_st = call_downstream(ctx, context); ! call_st) {
                            if (call_st.kind() == operation_status_kind::yield) {
                                ctx.resuming_calling_child_6_ = true;
                            }
                            return call_st;
                        }
                    }
                    (void) ctx.incr_.increment(secondary_group_index);
                }
                break;
            }
            case join_kind::anti: [[fallthrough]];
            case join_kind::semi: {
                if(cgrp.groups()[0].empty()) {
                    break;
                }
                do {
                    ctx.exists_match_ = false;
                    if(groups_available(cgrp, true)) {
                        do {
                            expr::evaluator_context c{
                                ctx.varlen_resource(),
                                ctx.req_context() ? ctx.req_context()->transaction().get() : nullptr
                            };
                            c.blob_session(std::addressof(helper.blob_session_container()));
                            auto a = assign_and_evaluate_condition(ctx, cgrp, ctx.incr_, c);
                            if (a.error()) {
                                handle_expression_error(*ctx.req_context(), a, c);
                                ctx.abort();
                                return operation_status_kind::aborted;
                            }
                            if (a.template to<bool>()) {
                                ctx.exists_match_ = true;
                                break;
                            }
                        } while (ctx.incr_.increment(secondary_group_index));
                        ctx.incr_.reset(secondary_group_index);
                    }
                    if((ctx.exists_match_ && kind_ == join_kind::semi) || (! ctx.exists_match_ && kind_ == join_kind::anti)) {
                        assign_values(ctx, cgrp, ctx.incr_, true);
resume_calling_child_7:
                        if(auto call_st = call_downstream(ctx, context); ! call_st) {
                            return call_st;
                        }
                    }
                } while (ctx.incr_.increment(primary_group_index));
                break;
            }
        }
        return operation_status_kind::ok;
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
        if (auto* p = find_context<join_context<iterator>>(index(), ctx.contexts())) {
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

}  // namespace jogasaki::executor::process::impl::ops
