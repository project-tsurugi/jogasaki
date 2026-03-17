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
#include "values.h"

#include <ostream>
#include <utility>
#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/relation/values.h>
#include <takatori/type/data.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <takatori/util/infect_qualifier.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/data/small_record_store.h>
#include <jogasaki/executor/conv/require_conversion.h>
#include <jogasaki/executor/expr/details/cast_evaluation.h>
#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/expr/evaluator_context.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/details/error_abort.h>
#include <jogasaki/executor/process/impl/ops/details/expression_error.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/process/impl/variable_table_info.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/request_cancel_config.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/cancel_request.h>

#include "cancel_if_needed.h"
#include "context_helper.h"
#include "operator_base.h"
#include "values_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

values::values(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    takatori::relation::values const& node,
    std::unique_ptr<operator_base> downstream
) :
    record_operator(index, info, block_index),
    downstream_(std::move(downstream))
{
    for (auto&& col : node.columns()) {
        variables_.emplace_back(col);
    }
    for (auto&& row : node.rows()) {
        auto& rev = row_evaluators_.emplace_back();
        auto& src_types = row_source_types_.emplace_back();
        for (auto&& elem : row.elements()) {
            rev.emplace_back(elem, info.compiled_info(), info.host_variables());
            src_types.emplace_back(&info.compiled_info().type_of(elem));
        }
    }
}

operation_status values::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<values_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<values_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status values::operator()(values_context& ctx, abstract::task_context* context) {
    if (ctx.aborted()) {
        return operation_status_kind::aborted;
    }
    auto& vars = ctx.output_variables();
    auto ref = vars.store().ref();
    auto& cinfo = compiled_info();
    auto cancel_enabled = utils::request_cancel_enabled(request_cancel_kind::values);
    while (ctx.current_row_ < row_evaluators_.size()) {
        if (ctx.state() != context_state::calling_child) {
            if (cancel_enabled && cancel_if_needed(ctx)) {
                finish(context);
                return operation_status_kind::aborted;
            }
            auto& rev = row_evaluators_[ctx.current_row_];
            for (std::size_t i = 0, n = variables_.size(); i < n; ++i) {
                auto& v = variables_[i];
                auto info = vars.info().at(v);
                auto& ev = rev[i];
                expr::evaluator_context c{
                    ctx.varlen_resource(),
                    ctx.req_context() ? ctx.req_context()->transaction().get() : nullptr
                };
                context_helper helper{ctx.task_context()};
                c.blob_session(std::addressof(helper.blob_session_container()));
                auto result = ev(c, vars, ctx.varlen_resource());
                if (result.error()) {
                    finish(context);
                    return handle_expression_error(ctx, result, c);
                }
                // perform implicit type coercion when the expression result type differs
                // from the column variable type (e.g. INT8 literal in a DECIMAL column)
                auto& source_type = *row_source_types_[ctx.current_row_][i];
                auto& target_type = cinfo.type_of(v);
                data::any converted = result;
                if (conv::to_require_conversion(source_type, target_type)) {
                    auto cast_result = expr::details::conduct_cast(c, source_type, target_type, result);
                    if (cast_result.error()) {
                        finish(context);
                        return handle_expression_error(ctx, cast_result, c);
                    }
                    converted = cast_result;
                }
                using t = takatori::type::type_kind;
                bool is_null = converted.empty();
                ref.set_null(info.nullity_offset(), is_null);
                if (! is_null) {
                    switch (target_type.kind()) {
                        case t::boolean: copy_to<runtime_t<meta::field_type_kind::boolean>>(ref, info.value_offset(), converted); break;
                        case t::int4: copy_to<runtime_t<meta::field_type_kind::int4>>(ref, info.value_offset(), converted); break;
                        case t::int8: copy_to<runtime_t<meta::field_type_kind::int8>>(ref, info.value_offset(), converted); break;
                        case t::float4: copy_to<runtime_t<meta::field_type_kind::float4>>(ref, info.value_offset(), converted); break;
                        case t::float8: copy_to<runtime_t<meta::field_type_kind::float8>>(ref, info.value_offset(), converted); break;
                        case t::decimal: copy_to<runtime_t<meta::field_type_kind::decimal>>(ref, info.value_offset(), converted); break;
                        case t::character: copy_to<runtime_t<meta::field_type_kind::character>>(ref, info.value_offset(), converted); break;
                        case t::octet: copy_to<runtime_t<meta::field_type_kind::octet>>(ref, info.value_offset(), converted); break;
                        case t::date: copy_to<runtime_t<meta::field_type_kind::date>>(ref, info.value_offset(), converted); break;
                        case t::time_of_day: copy_to<runtime_t<meta::field_type_kind::time_of_day>>(ref, info.value_offset(), converted); break;
                        case t::time_point: copy_to<runtime_t<meta::field_type_kind::time_point>>(ref, info.value_offset(), converted); break;
                        case t::blob: copy_to<runtime_t<meta::field_type_kind::blob>>(ref, info.value_offset(), converted); break;
                        case t::clob: copy_to<runtime_t<meta::field_type_kind::clob>>(ref, info.value_offset(), converted); break;
                        default:
                            finish(context);
                            VLOG_LP(log_error) << "Unsupported type in values operator result:"
                                               << cinfo.type_of(v).kind();
                            return error_abort(ctx, status::err_unsupported);
                    }
                }
            }
        } else {
            VLOG_LP(log_trace) << "resuming values op. after downstream yield";
        }
        if (downstream_) {
            ctx.state(context_state::calling_child);
            auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
            if (st.kind() == operation_status_kind::yield) {
                return operation_status_kind::yield;
            }
            if (st.kind() == operation_status_kind::aborted) {
                ctx.abort();
                finish(context);
                return operation_status_kind::aborted;
            }
            ctx.state(context_state::running_operator_body);
        }
        ++ctx.current_row_;
    }
    finish(context);
    return operation_status_kind::ok;
}

operator_kind values::kind() const noexcept {
    return operator_kind::values;
}

void values::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if (auto* p = find_context<values_context>(index(), ctx.contexts())) {
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

}  // namespace jogasaki::executor::process::impl::ops
