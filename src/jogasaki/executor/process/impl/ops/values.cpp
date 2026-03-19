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
#include <takatori/util/downcast.h>
#include <takatori/util/infect_qualifier.h>
#include <yugawara/compiled_info.h>

#include <jogasaki/executor/expr/evaluator.h>
#include <jogasaki/executor/process/impl/ops/context_container.h>
#include <jogasaki/executor/process/impl/ops/details/error_abort.h>
#include <jogasaki/executor/process/impl/variable_table.h>
#include <jogasaki/executor/wrt/fill_evaluated_value.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/meta/field_type_kind.h>
#include <jogasaki/meta/field_type_traits.h>
#include <jogasaki/request_cancel_config.h>
#include <jogasaki/status.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/field_types.h>

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
    fields_(create_fields(node.columns(), info)),
    downstream_(std::move(downstream))
{
    row_evaluators_.reserve(node.rows().size());
    row_source_types_.reserve(node.rows().size());
    for (auto&& row : node.rows()) {
        auto& revals = row_evaluators_.emplace_back();
        auto& src_types = row_source_types_.emplace_back();
        revals.reserve(row.elements().size());
        src_types.reserve(row.elements().size());
        for (auto&& elem : row.elements()) {
            revals.emplace_back(elem, info.compiled_info(), info.host_variables());
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

    auto cancel_enabled = utils::request_cancel_enabled(request_cancel_kind::values);
    while (ctx.current_row_ < row_evaluators_.size()) {
        if (ctx.state() != context_state::calling_child) {
            if (cancel_enabled && cancel_if_needed(ctx)) {
                finish(context);
                return operation_status_kind::aborted;
            }
            auto& revals = row_evaluators_[ctx.current_row_];
            context_helper helper{ctx.task_context()};
            process::impl::variable_table empty{};
            for (std::size_t i = 0, n = fields_.size(); i < n; ++i) {
                auto const& field = fields_[i];
                if (auto st = wrt::fill_evaluated_value(
                        revals[i],
                        *row_source_types_[ctx.current_row_][i],
                        wrt::value_input_conversion_kind::unify,
                        field,
                        *ctx.req_context(),
                        helper.blob_session_container(),
                        empty,
                        *ctx.varlen_resource(),
                        ref
                    );
                    st != status::ok) {
                    ctx.abort();
                    finish(context);
                    return operation_status_kind::aborted;
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

std::vector<details::values_field> values::create_fields(
    std::vector<takatori::descriptor::variable> const& columns,
    processor_info const& pinfo
) {
    std::vector<details::values_field> fields{};
    fields.reserve(columns.size());
    for (auto const& col : columns) {
        auto const& target_type = pinfo.compiled_info().type_of(col);
        auto const& vinfo = block_info().at(col);
        fields.emplace_back(details::values_field{
            utils::type_for(target_type),
            true,  // always nullable, so values op. does not cause not-null violation
            vinfo.value_offset(),
            vinfo.nullity_offset(),
            std::addressof(target_type)
        });
    }
    return fields;
}

}  // namespace jogasaki::executor::process::impl::ops
