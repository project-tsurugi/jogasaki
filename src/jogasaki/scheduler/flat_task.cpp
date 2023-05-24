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

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/transaction.h>
#include <tateyama/api/task_scheduler/context.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/file/loader.h>
#include <jogasaki/utils/trace_log.h>
#include <jogasaki/utils/hex.h>
#include <jogasaki/request_logging.h>

#ifdef PERFORMANCE_TOOLS
#include <performance-tools/marker.h>
#endif

namespace jogasaki::scheduler {

void flat_task::bootstrap(tateyama::api::task_scheduler::context& ctx) {
#ifdef PERFORMANCE_TOOLS
    MARKER_SCOPE("flat_task::bootstrap");
#endif
    log_entry << *this;
    trace_scope_name("bootstrap");  //NOLINT
    job()->preferred_worker_index().store(ctx.index());
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*req_context_->stmt_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.init(*graph_, *req_context_);
    dc.process_internal_events();
    log_exit << *this;
}

void flat_task::dag_schedule() {
#ifdef PERFORMANCE_TOOLS
    MARKER_SCOPE("flat_task::dag_schedule");
#endif
    log_entry << *this;
    trace_scope_name("dag_schedule");  //NOLINT
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*req_context_->stmt_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.process_internal_events();
    log_exit << *this;
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

void print_task_diagnostic(const flat_task &t, std::ostream &os) {
    os << std::boolalpha;
    os << "        - id: " << utils::hex(t.id()) << std::endl;
    os << "          kind: " << t.kind() << std::endl;
    os << "          sticky: " << t.sticky() << std::endl;
    os << "          delayed: " << t.delayed() << std::endl;
    if(t.req_context() && t.req_context()->job()) {
        os << "          job_id: " << utils::hex(t.req_context()->job()->id()) << std::endl;
    }
}

void flat_task::resubmit(request_context& req_context) {
    auto& ts = *req_context.scheduler();
    ts.schedule_task(flat_task{*this});
}

bool flat_task::teardown() {
#ifdef PERFORMANCE_TOOLS
    MARKER_SCOPE("flat_task::teardown");
#endif
    log_entry << *this;
    trace_scope_name("teardown");  //NOLINT
    bool ret = true;
    if (auto cnt = job()->task_count().load(); cnt > 1) {
        DVLOG_LP(log_debug) << *this << " other " << cnt << " tasks remain and teardown is rescheduled.";
        submit_teardown(*req_context_, true);
        ret = false;
    }
    log_exit << *this << " ret:" << ret;
    return ret;
}

void flat_task::write() {
#ifdef PERFORMANCE_TOOLS
    MARKER_SCOPE("flat_task::write");
#endif
    log_entry << *this;
    trace_scope_name("write");  //NOLINT
    (*write_)(*req_context_);
    submit_teardown(*req_context_);
    log_exit << *this;
}

bool flat_task::execute(tateyama::api::task_scheduler::context& ctx) {
    // The variables begin and end are needed only for timing event log.
    // Avoid calling clock::now() if log level is low. In case its cost is unexpectedly high.
    std::chrono::time_point<clock> begin{};
    if (VLOG_IS_ON(log_debug_timing_event_fine)) {
        begin = clock::now();
    }
    bool ret = false;
    switch(kind_) {
        using kind = flat_task_kind;
        case kind::dag_events: dag_schedule(); break;
        case kind::bootstrap: bootstrap(ctx); break;
        case kind::resolve: resolve(ctx); break;
        case kind::teardown: ret = teardown(); break;
        case kind::wrapped: execute_wrapped(); break;
        case kind::write: write(); break;
        case kind::load: load(); break;
    }
    std::chrono::time_point<clock> end{};
    if (VLOG_IS_ON(log_debug_timing_event_fine)) {
        end = clock::now();
    }
    if(auto req_detail = job()->request()) {
        req_detail->task_duration_ns() += std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count();
        ++req_detail->task_count();
        if(sticky_) {
            ++req_detail->sticky_task_count();
        }
        if(ctx.task_is_stolen()) {
            ++req_detail->task_steling_count();
        }
    }
    return ret;
}

void flat_task::finish_job() {
#ifdef PERFORMANCE_TOOLS
    MARKER_SCOPE("flat_task::finish_job");
#endif
    // job completed, and the latch needs to be released
    auto& ts = *req_context_->scheduler();
    auto& j = *job();
    auto& cb = j.callback();
    auto req_detail = j.request();
    if(cb) {
        cb();
    }
    if(req_detail) {
        req_detail->status(scheduler::request_detail_status::finishing);
        log_request(*req_detail, req_context_->status_code() == status::ok);

        VLOG(log_debug_timing_event_fine) << "/:jogasaki:metrics:task_time"
            << " job_id:" << utils::hex(req_detail->id())
            << " value:" << req_detail->task_duration_ns() / 1000 // print task time in us
            ;
        VLOG(log_debug_timing_event_fine) << "/:jogasaki:metrics:task_count"
            << " job_id:" << utils::hex(req_detail->id())
            << " value:" << req_detail->task_count()
            ;
        VLOG(log_debug_timing_event_fine) << "/:jogasaki:metrics:task_stealing_count"
            << " job_id:" << utils::hex(req_detail->id())
            << " value:" << req_detail->task_steling_count()
            ;
        VLOG(log_debug_timing_event_fine) << "/:jogasaki:metrics:sticky_task_count"
            << " job_id:" << utils::hex(req_detail->id())
            << " value:" << req_detail->sticky_task_count()
            ;
        VLOG(log_debug_timing_event_fine) << "/:jogasaki:metrics:sticky_task_worker_enforced_count"
            << " job_id:" << utils::hex(req_detail->id())
            << " value:" << req_detail->sticky_task_worker_enforced_count()
            ;
    }
    j.completion_latch().release();

    // after the unregister, job should not be touched as it's possibly released
    ts.unregister_job(j.id());
    // here job context is released and objects held by job callback such as request_context are also released
}

void flat_task::operator()(tateyama::api::task_scheduler::context& ctx) {
#ifdef PERFORMANCE_TOOLS
    MARKER_SCOPE("flat_task::operator()");
#endif
    auto started = job()->started().load();
    if(! started) {
        if(job()->started().compare_exchange_strong(started, true)) {
            if(auto req = job()->request()) {
                req->status(scheduler::request_detail_status::executing);
                log_request(*req);
            }
        }
    }
    auto job_completes = execute(ctx);
    if(auto& tctx = req_context_->transaction(); tctx && sticky_) {
        tctx->decrement_worker_count();
    }
    auto jobid = job()->id();
    auto cnt = --job()->task_count();
    // Be careful and don't touch job or request contexts after decrementing the counter which makes teardown job to finish.
    (void)cnt;
    (void)jobid;
    //VLOG(log_debug) << "decremented job " << jobid << " task count to " << cnt;
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
    std::shared_ptr<model::task> origin,
    bool delayed
) noexcept:
    kind_(flat_task_kind::wrapped),
    req_context_(rctx),
    origin_(std::move(origin)),
    sticky_(origin_->has_transactional_io()),
    delayed_(delayed)
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
#ifdef PERFORMANCE_TOOLS
    MARKER_SCOPE("flat_task::resolve");
#endif
    log_entry << *this;
    (void)ctx;
    auto& e = sctx_->executable_statement_;
    if(auto res = sctx_->database_->resolve(sctx_->prepared_,
            maybe_shared_ptr{sctx_->parameters_}, e); res != status::ok) {
        req_context_->status_code(res);
    } else {
        sctx_->tx_->execute_async_on_context(
            req_context_.ownership(),
            maybe_shared_ptr{e.get()},
            [sctx=sctx_](status st, std::string_view msg){
                sctx->callback_(st, msg);
            }, false);
    }
    log_exit << *this;
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
    req_context_(std::move(rctx)),
    sctx_(std::move(sctx))
{}

void flat_task::load() {
#ifdef PERFORMANCE_TOOLS
    MARKER_SCOPE("flat_task::load");
#endif
    log_entry << *this;
    auto res = (*loader_)();
    if(res == executor::file::loader_result::running) {
        auto& ts = *req_context_->scheduler();
        ts.schedule_task(flat_task{
            task_enum_tag<flat_task_kind::load>,
            req_context_.get(),
            loader_
        });
    } else {
        if(res == executor::file::loader_result::error) {
            auto [st, msg] = loader_->error_info();
            req_context_->status_code(st, std::move(msg));
        }
        submit_teardown(*req_context_);
    }
    log_exit << *this;
}

flat_task::flat_task(task_enum_tag_t<flat_task_kind::load>, request_context* rctx,
    std::shared_ptr<executor::file::loader> ldr) noexcept:
    kind_(flat_task_kind::load),
    req_context_(rctx),
    loader_(std::move(ldr))
{}

void flat_task::execute_wrapped() {
#ifdef PERFORMANCE_TOOLS
    MARKER_SCOPE("flat_task::execute_wrapped");
#endif
    //DVLOG(log_trace) << *this << " wrapped task executed.";
    trace_scope_name("executor_task");  //NOLINT
    model::task_result res{};
    while((res = (*origin_)()) == model::task_result::proceed) {}
    if(res == model::task_result::yield) {
        resubmit(*req_context_);
        return;
    }
}

}



