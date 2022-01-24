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

#include <jogasaki/logging.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <tateyama/api/task_scheduler/context.h>
#include <jogasaki/executor/common/execute.h>

namespace jogasaki::scheduler {

using takatori::util::fail;

void flat_task::bootstrap(tateyama::api::task_scheduler::context& ctx) {
    DVLOG(log_trace) << *this << " bootstrap task executed.";
    trace_scope_name("bootstrap");  //NOLINT
    job()->index().store(ctx.index());
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*req_context_->stmt_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.init(*graph_, *req_context_);
    dc.process_internal_events();
}

void flat_task::dag_schedule() {
    DVLOG(log_trace) << *this << " dag scheduling task executed.";
    trace_scope_name("dag_schedule");  //NOLINT
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*req_context_->stmt_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.process_internal_events();
}

bool flat_task::teardown() {
    DVLOG(log_trace) << *this << " teardown task executed.";
    trace_scope_name("teardown");  //NOLINT
    if (job()->task_count() > 1) {
        DVLOG(log_debug) << *this << " other tasks remain and teardown is rescheduled.";
        auto& ts = *req_context_->scheduler();
        ts.schedule_task(flat_task{task_enum_tag<flat_task_kind::teardown>, req_context_});
        return true;
    }
    auto& cb = job()->callback();
    if(cb) {
        cb();
    }

    // releasing latch should be done here at the last step working on job context
    // since it starts to release resources such as request context
    job()->completion_latch().release();

    // we rely on callback to own request_context, but somehow it fails to release.
    // So temporarily we explicitly release the callback object. TODO investigate more
    std::function<void(void)>{}.swap(cb);
    return false;
}

void flat_task::write() {
    DVLOG(log_trace) << *this << " write task executed.";
    trace_scope_name("write");  //NOLINT
    (*write_)(*req_context_);

    auto& cb = job()->callback();
    if(cb) {
        cb();
    }

    // releasing latch should be done at the last step since it starts to release resources such as request context
    job()->completion_latch().release();

    // we rely on callback to own request_context, but somehow it fails to release.
    // So temporarily we explicitly release the callback object. TODO investigate more
    std::function<void(void)>{}.swap(cb);
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
        case kind::write: write(); return false;
    }
    fail();
}

void flat_task::operator()(tateyama::api::task_scheduler::context& ctx) {
    if(! execute(ctx)) {
        // job completed, and the latch is just released. Should not touch the job context any more.
        return;
    }
    --job()->task_count();
}

flat_task::identity_type flat_task::id() const {
    if (origin_) {
        return origin_->id();
    }
    return undefined_id;
}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::wrapped>,
    request_context* rctx,
    std::shared_ptr<model::task> origin
) noexcept:
    kind_(flat_task_kind::wrapped),
    req_context_(rctx),
    origin_(std::move(origin)),
    sticky_(origin_->has_transactional_io())
{}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::dag_events>,
    request_context* rctx
) noexcept:
    kind_(flat_task_kind::dag_events),
    req_context_(rctx)
{}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::bootstrap>,
    request_context* rctx,
    model::graph& g
) noexcept:
    kind_(flat_task_kind::bootstrap),
    req_context_(rctx),
    graph_(std::addressof(g))
{}

std::shared_ptr<model::task> const& flat_task::origin() const noexcept {
    return origin_;
}

job_context* flat_task::job() const {
    return req_context_->job().get();
}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::teardown>,
    request_context* rctx
) noexcept:
    kind_(flat_task_kind::teardown),
    req_context_(rctx)
{}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::write>,
    request_context* rctx,
    executor::common::write* write
) noexcept:
    kind_(flat_task_kind::write),
    req_context_(rctx),
    write_(write),
    sticky_(true)
{}

bool flat_task::sticky() const noexcept {
    return sticky_;
}

request_context* flat_task::req_context() const noexcept {
    return req_context_;
}

}



