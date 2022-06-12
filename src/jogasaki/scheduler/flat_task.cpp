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
#include <takatori/util/downcast.h>

#include <jogasaki/logging.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/transaction.h>
#include <tateyama/api/task_scheduler/context.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/execute.h>

namespace jogasaki::scheduler {

using takatori::util::fail;
using takatori::util::unsafe_downcast;

void flat_task::bootstrap(tateyama::api::task_scheduler::context& ctx) {
    DVLOG(log_trace) << *this << " bootstrap task executed.";
    trace_scope_name("bootstrap");  //NOLINT
    job()->preferred_worker_index().store(ctx.index());
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

void submit_teardown(request_context& req_context, bool force) {
    // make sure teardown task is submitted only once
    auto& ts = *req_context.scheduler();
    auto& job = *req_context.job();
    auto completing = job.completing().load();
    if (force || (!completing && job.completing().compare_exchange_strong(completing, true))) {
        ts.schedule_task(flat_task{task_enum_tag<flat_task_kind::teardown>, std::addressof(req_context)});
    }
}

bool flat_task::teardown() {
    DVLOG(log_trace) << *this << " teardown task executed.";
    trace_scope_name("teardown");  //NOLINT
    if (auto cnt = job()->task_count().load(); cnt > 1) {
        DVLOG(log_debug) << *this << " other " << cnt << " tasks remain and teardown is rescheduled.";
        submit_teardown(*req_context_, true);
        return false;
    }
    return true;
}

void flat_task::write() {
    DVLOG(log_trace) << *this << " write task executed.";
    trace_scope_name("write");  //NOLINT
    (*write_)(*req_context_);
    submit_teardown(*req_context_);
}

bool flat_task::execute(tateyama::api::task_scheduler::context& ctx) {
    switch(kind_) {
        using kind = flat_task_kind;
        case kind::dag_events: dag_schedule(); return false;
        case kind::bootstrap: bootstrap(ctx); return false;
        case kind::resolve: resolve(ctx); return false;
        case kind::teardown: return teardown();
        case kind::wrapped: {
            trace_scope_name("executor_task");  //NOLINT
            while((*origin_)() == model::task_result::proceed) {}
            return false;
        }
        case kind::write: write(); return false;
        case kind::load: load(); return false;
    }
    fail();
}

void flat_task::finish_job() {
    // job completed, and the latch needs to be released
    auto& ts = *req_context_->scheduler();
    auto& j = *job();
    auto& cb = j.callback();
    if(cb) {
        cb();

        // we rely on callback to own request_context, but somehow it fails to release.
        // So temporarily we explicitly release the callback object. TODO investigate more
        std::function<void(void)>{}.swap(cb);
    }

    // releasing latch should be done at the last step since it starts to release resources such as request context
    j.completion_latch().release();

    ts.unregister_job(j.id());
}

void flat_task::operator()(tateyama::api::task_scheduler::context& ctx) {
    auto job_completes = execute(ctx);
    auto idx = job()->preferred_worker_index().load();
    if (idx == job_context::undefined_index) {
        if(auto& tctx = req_context_->transaction(); tctx && sticky_) {
            tctx->decrement_worker_count();
        }
    }
    auto cnt = --job()->task_count();
    (void)cnt;
    VLOG(log_debug) << "decremented job " << job()->id() << " task count to " << cnt;
    if(! job_completes) {
        return;
    }
    finish_job();
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

void flat_task::resolve(tateyama::api::task_scheduler::context& ctx) {
    (void)ctx;
    auto& e = sctx_->executable_statement_;
    if(auto res = sctx_->database_->resolve(sctx_->prepared_,
            maybe_shared_ptr{sctx_->parameters_}, e); res != status::ok) {
        req_context_->status_code(res);
        return;
    }
    sctx_->tx_->execute_context(
        req_context_.ownership(),
        maybe_shared_ptr{e.get()}, sctx_->channel_, [sctx=sctx_](status st, std::string_view msg){
        (void)sctx;
        sctx->callback_(st, msg);
    }, false);
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
    return req_context_.get();
}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::resolve>,
    std::shared_ptr<request_context> rctx,
    std::shared_ptr<statement_context> sctx
) noexcept:
    kind_(flat_task_kind::resolve),
    req_context_(rctx),
    sctx_(std::move(sctx))
{}

void flat_task::load() {
    if ((*loader_)()) {
        auto& ts = *req_context_->scheduler();
        ts.schedule_task(flat_task{
            task_enum_tag<flat_task_kind::load>,
            req_context_.get(),
            loader_
        });
        return;
    }
    submit_teardown(*req_context_);
}

flat_task::flat_task(task_enum_tag_t<flat_task_kind::load>, request_context* rctx,
    std::shared_ptr<executor::file::loader> ldr) noexcept:
    kind_(flat_task_kind::load),
    req_context_(rctx),
    sticky_(false),
    loader_(std::move(ldr))
{}

}



