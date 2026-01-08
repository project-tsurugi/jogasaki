/*
 * Copyright 2018-2026 Project Tsurugi.
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
#include "apply.h"

#include <ostream>
#include <utility>

#include <boost/assert.hpp>
#include <glog/logging.h>

#include <takatori/relation/apply.h>
#include <takatori/type/data.h>
#include <takatori/type/type_kind.h>
#include <takatori/util/downcast.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/data/any_sequence.h>
#include <jogasaki/data/any_sequence_stream.h>
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
#include <jogasaki/status.h>

#include "apply_context.h"
#include "context_helper.h"
#include "operator_base.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

apply::apply(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    takatori::relation::apply_kind operator_kind,
    function::table_valued_function_info const* function_info,
    std::vector<takatori::relation::details::apply_column> const& columns,
    std::vector<expr::evaluator> arguments,
    std::unique_ptr<operator_base> downstream
) :
    record_operator(index, info, block_index),
    operator_kind_(operator_kind),
    function_info_(function_info),
    argument_evaluators_(std::move(arguments)),
    downstream_(std::move(downstream))
{
    column_positions_.reserve(columns.size());
    column_variables_.reserve(columns.size());
    for (auto const& col : columns) {
        column_positions_.emplace_back(col.position());
        column_variables_.emplace_back(col.variable());
    }
}

operation_status apply::process_record(abstract::task_context* context) {
    BOOST_ASSERT(context != nullptr);  //NOLINT
    context_helper ctx{*context};
    auto* p = find_context<apply_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<apply_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status apply::operator()(apply_context& ctx, abstract::task_context* context) {  //NOLINT(readability-function-cognitive-complexity)
    if (ctx.inactive()) {
        return {operation_status_kind::aborted};
    }

    // setup evaluator context blob session once per record
    context_helper helper{*context};
    ctx.evaluator_context_.blob_session(std::addressof(helper.blob_session_container()));

    // evaluate arguments
    ctx.args_.clear();
    ctx.args_.reserve(argument_evaluators_.size());
    if (! evaluate_arguments(ctx, ctx.args_)) {
        ctx.abort();
        return {operation_status_kind::aborted};
    }

    // call table-valued function
    if (! function_info_ || ! function_info_->function_body()) {
        VLOG_LP(log_error) << "Table-valued function info is not set";
        return error_abort(ctx, status::err_unknown);
    }

    auto stream = function_info_->function_body()(
        ctx.evaluator_context_,
        takatori::util::sequence_view<data::any>{ctx.args_.data(), ctx.args_.size()}
    );

    if (! stream) {
        VLOG_LP(log_error) << "Table-valued function returned null stream";
        return error_abort(ctx, status::err_unknown);
    }

    // synchronously collect all results
    ctx.has_output_ = false;
    data::any_sequence sequence{};

    while (true) {
        sequence.clear();
        auto status = stream->next(sequence, std::nullopt);

        if (status == data::any_sequence_stream_status::end_of_stream) {
            break;
        }

        if (status == data::any_sequence_stream_status::error) {
            VLOG_LP(log_error) << "Error while reading from table-valued function stream";
            stream->close();
            ctx.abort();
            return {operation_status_kind::aborted};
        }

        if (status == data::any_sequence_stream_status::ok) {
            // assign sequence values to output variables
            assign_sequence_to_variables(ctx, sequence);
            ctx.has_output_ = true;

            // call downstream
            if (downstream_) {
                if (auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); ! st) {
                    stream->close();
                    ctx.abort();
                    return {operation_status_kind::aborted};
                }
            }
        }
    }

    // for OUTER APPLY: if no rows were output, emit NULL row
    if (operator_kind_ == takatori::relation::apply_kind::outer && ! ctx.has_output_) {
        assign_null_to_variables(ctx);

        if (downstream_) {
            if (auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context); ! st) {
                stream->close();
                ctx.abort();
                return {operation_status_kind::aborted};
            }
        }
    }

    stream->close();
    return {};
}

operator_kind apply::kind() const noexcept {
    return operator_kind::apply;
}

void apply::finish(abstract::task_context* context) {
    if (! context) {
        return;
    }
    context_helper ctx{*context};
    if (auto* p = find_context<apply_context>(index(), ctx.contexts())) {
        p->release();
    }
    if (downstream_) {
        downstream_->finish(context);
    }
}

bool apply::evaluate_arguments(apply_context& ctx, std::vector<data::any>& args) {
    auto& vars = ctx.output_variables();

    for (auto& ev : argument_evaluators_) {
        auto result = ev(ctx.evaluator_context_, vars, ctx.varlen_resource());
        if (result.error()) {
            VLOG_LP(log_error) << "Error evaluating apply operator argument";
            return false;
        }
        args.emplace_back(result);
    }
    return true;
}

void apply::assign_sequence_to_variables(apply_context& ctx, data::any_sequence const& sequence) {
    auto& vars = ctx.output_variables();
    auto ref = vars.store().ref();
    auto& cinfo = compiled_info();

    for (std::size_t i = 0; i < column_positions_.size(); ++i) {
        auto pos = column_positions_[i];
        auto const& var = column_variables_[i];

        if (pos >= sequence.size()) {
            VLOG_LP(log_warning) << "Column position " << pos << " exceeds sequence size " << sequence.size();
            continue;
        }

        auto const& value = sequence[pos];
        auto info = vars.info().at(var);

        bool is_null = value.empty();
        ref.set_null(info.nullity_offset(), is_null);

        if (! is_null) {
            using t = takatori::type::type_kind;
            switch (cinfo.type_of(var).kind()) {
                case t::boolean:
                    ref.set_value<runtime_t<meta::field_type_kind::boolean>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::boolean>>());
                    break;
                case t::int4:
                    ref.set_value<runtime_t<meta::field_type_kind::int4>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::int4>>());
                    break;
                case t::int8:
                    ref.set_value<runtime_t<meta::field_type_kind::int8>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::int8>>());
                    break;
                case t::float4:
                    ref.set_value<runtime_t<meta::field_type_kind::float4>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::float4>>());
                    break;
                case t::float8:
                    ref.set_value<runtime_t<meta::field_type_kind::float8>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::float8>>());
                    break;
                case t::decimal:
                    ref.set_value<runtime_t<meta::field_type_kind::decimal>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::decimal>>());
                    break;
                case t::character:
                    ref.set_value<runtime_t<meta::field_type_kind::character>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::character>>());
                    break;
                case t::octet:
                    ref.set_value<runtime_t<meta::field_type_kind::octet>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::octet>>());
                    break;
                case t::date:
                    ref.set_value<runtime_t<meta::field_type_kind::date>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::date>>());
                    break;
                case t::time_of_day:
                    ref.set_value<runtime_t<meta::field_type_kind::time_of_day>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::time_of_day>>());
                    break;
                case t::time_point:
                    ref.set_value<runtime_t<meta::field_type_kind::time_point>>(
                        info.value_offset(), value.to<runtime_t<meta::field_type_kind::time_point>>());
                    break;
                default:
                    VLOG_LP(log_warning) << "Unsupported type in apply operator: " << cinfo.type_of(var).kind();
                    break;
            }
        }
    }
}

void apply::assign_null_to_variables(apply_context& ctx) {
    auto& vars = ctx.output_variables();
    auto ref = vars.store().ref();

    for (auto const& var : column_variables_) {
        auto info = vars.info().at(var);
        ref.set_null(info.nullity_offset(), true);
    }
}

}  // namespace jogasaki::executor::process::impl::ops
