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
#include "flat_task.h"

#include <takatori/util/fail.h>

#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <tateyama/api/task_scheduler/context.h>
#include <jogasaki/executor/common/execute.h>

namespace jogasaki::scheduler {

using takatori::util::fail;

void flat_task::bootstrap(tateyama::api::task_scheduler::context& ctx) {
    DVLOG(1) << *this << " bootstrap task executed.";
    trace_scope_name("bootstrap");  //NOLINT
    job_context_->index().store(ctx.index());
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*job_context_->dag_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.init(*graph_);
    dc.process_internal_events();
}

void flat_task::dag_schedule() {
    DVLOG(1) << *this << " dag scheduling task executed.";
    trace_scope_name("dag_schedule");  //NOLINT
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*job_context_->dag_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.process_internal_events();
}

bool flat_task::teardown() {
    DVLOG(1) << *this << " teardown task executed.";
    trace_scope_name("teardown");  //NOLINT
    if (job_context_->task_count() > 1) {
        DVLOG(1) << *this << " other tasks remain and teardown is rescheduled.";
        auto& ts = job_context_->dag_scheduler()->get_task_scheduler();
        ts.schedule_task(flat_task{task_enum_tag<flat_task_kind::teardown>, job_context_});
        return true;
    }
    if (auto& cb = job_context_->callback(); cb) {
        cb();
    }

    // releasing latch should be done at the last step since it starts to release resources such as request context
    job_context_->completion_latch().release();
    return false;
}

bool flat_task::execute(tateyama::api::task_scheduler::context& ctx) {
    switch(kind_) {
        using kind = flat_task_kind;
        case kind::dag_events: dag_schedule(); return true;
        case kind::bootstrap: bootstrap(ctx); return true;
        case kind::teardown: return teardown();
        case kind::wrapped: {
            trace_scope_name("executor_task");  //NOLINT
            while((*origin_)() == model::task_result::proceed) {}
            return true;
        }
    }
    fail();
}

void flat_task::operator()(tateyama::api::task_scheduler::context& ctx) {
    if(! execute(ctx)) {
        // job completed, and the latch is just released. Should not touch the job context any more.
        return;
    }
    --job_context_->task_count();
}

flat_task::identity_type flat_task::id() const {
    if (origin_) {
        return origin_->id();
    }
    return undefined_id;
}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::wrapped>,
    job_context* jctx,
    std::shared_ptr<model::task> origin
) noexcept:
    kind_(flat_task_kind::wrapped),
    job_context_(jctx),
    origin_(std::move(origin))
{}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::dag_events>,
    job_context* jctx
) noexcept:
    kind_(flat_task_kind::dag_events),
    job_context_(jctx)
{}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::bootstrap>,
    job_context* jctx,
    model::graph& g
) noexcept:
    kind_(flat_task_kind::bootstrap),
    job_context_(jctx),
    graph_(std::addressof(g))
{}

std::shared_ptr<model::task> const& flat_task::origin() const noexcept {
    return origin_;
}

job_context* flat_task::job() const {
    return job_context_;
}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::teardown>,
    job_context* jctx
) noexcept:
    kind_(flat_task_kind::teardown),
    job_context_(jctx)
{}

}



