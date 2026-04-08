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
#include "project.h"

#include <ostream>
#include <utility>
#include <glog/logging.h>

#include <takatori/relation/project.h>
#include <takatori/type/data.h>
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
#include <jogasaki/status.h>
#include <jogasaki/utils/assert.h>
#include <jogasaki/utils/field_types.h>

#include "context_helper.h"
#include "operator_base.h"
#include "project_context.h"

namespace jogasaki::executor::process::impl::ops {

using takatori::util::unsafe_downcast;

project::project(
    operator_base::operator_index_type index,
    processor_info const& info,
    operator_base::block_index_type block_index,
    takatori::tree::tree_fragment_vector<takatori::relation::project::column> const& columns,
    std::unique_ptr<operator_base> downstream
) :
    record_operator(index, info, block_index),
    fields_(create_fields(columns, info)),
    downstream_(std::move(downstream))
{
    evaluators_.reserve(columns.size());
    for(auto&& c: columns) {
        evaluators_.emplace_back(c.value(), info.compiled_info(), info.host_variables());
    }
}

operation_status project::process_record(abstract::task_context* context) {
    assert_with_exception(context != nullptr, context);
    context_helper ctx{*context};
    auto* p = find_context<project_context>(index(), ctx.contexts());
    if (! p) {
        p = ctx.make_context<project_context>(
            index(),
            ctx.variable_table(block_index()),
            ctx.resource(),
            ctx.varlen_resource()
        );
    }
    return (*this)(*p, context);
}

operation_status project::operator()(project_context& ctx, abstract::task_context* context) {
    assert_with_exception(ctx.state() != context_state::yielding, ctx.state());
    if (ctx.aborted()) {
        return operation_status_kind::aborted;
    }
    if (ctx.state() != context_state::calling_child) {
        auto& vars = ctx.output_variables();
        // fill scope variables
        auto ref = vars.store().ref();
        context_helper helper{ctx.task_context()};
        for(std::size_t i=0, n = fields_.size(); i < n; ++i) {
            if (auto st = wrt::fill_evaluated_value(
                    evaluators_[i],
                    *fields_[i].target_type_,
                    wrt::value_input_conversion_kind::none,
                    fields_[i],
                    *ctx.req_context(),
                    helper.blob_session_container(),
                    vars,
                    *ctx.varlen_resource(),
                    ref
                );
                st != status::ok) {
                ctx.abort();
                return operation_status_kind::aborted;
            }
        }
    } else {
        VLOG_LP(log_trace) << "resuming project op. after downstream yield";
    }
    if (downstream_) {
        ctx.state(context_state::calling_child);
        auto st = unsafe_downcast<record_operator>(downstream_.get())->process_record(context);
        if (st.kind() == operation_status_kind::yield) {
            return operation_status_kind::yield;
        }
        if (st.kind() == operation_status_kind::aborted) {
            ctx.abort();
            return operation_status_kind::aborted;
        }
        ctx.state(context_state::running_operator_body);
    }
    return operation_status_kind::ok;
}

operator_kind project::kind() const noexcept {
    return operator_kind::project;
}

void project::finish(abstract::task_context* context) {
    if (! context) return;
    context_helper ctx{*context};
    if (auto* p = find_context<project_context>(index(), ctx.contexts())) {
        p->release();
    }
    if (downstream_) {
        unsafe_downcast<record_operator>(downstream_.get())->finish(context);
    }
}

std::vector<details::project_field> project::create_fields(
    takatori::tree::tree_fragment_vector<takatori::relation::project::column> const& columns,
    processor_info const& pinfo
) {
    std::vector<details::project_field> fields{};
    fields.reserve(columns.size());
    for (auto&& c : columns) {
        auto const& target_type = pinfo.compiled_info().type_of(c.variable());
        auto const& vinfo = block_info().at(c.variable());
        fields.emplace_back(details::project_field{
            utils::type_for(target_type),
            true,  // always nullable, so project op. does not cause not-null violation
            vinfo.value_offset(),
            vinfo.nullity_offset(),
            std::addressof(target_type)
        });
    }
    return fields;
}

}  // namespace jogasaki::executor::process::impl::ops
