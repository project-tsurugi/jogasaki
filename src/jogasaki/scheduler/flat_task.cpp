/*
 * Copyright 2018-2023 Project Tsurugi.
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

#include <atomic>
#include <glog/logging.h>
#include <jogasaki/commit_common.h>
#include <mutex>
#include <type_traits>

#include <takatori/util/string_builder.h>
#include <tateyama/common.h>
#include <tateyama/logging_helper.h>
#include <tateyama/task_scheduler/context.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/error_code.h>
#include <jogasaki/executor/common/write_statement.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/file/loader.h>
#include <jogasaki/logging.h>
#include <jogasaki/model/graph.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_logging.h>
#include <jogasaki/scheduler/dag_controller.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/scheduler/schedule_option.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/scheduler/thread_local_info.h>
#include <jogasaki/utils/cancel_request.h>
#include <jogasaki/utils/hex.h>
#include <jogasaki/utils/latch.h>
#include <jogasaki/utils/trace_log.h>

namespace jogasaki::scheduler {

using takatori::util::string_builder;

void flat_task::bootstrap(tateyama::task_scheduler::context&) {
    log_entry << *this;
    trace_scope_name("bootstrap");  //NOLINT
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*req_context_->stmt_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.init(*graph_, *req_context_);
    dc.process_internal_events();
    log_exit << *this;
}

void dag_schedule(request_context& req_context) {
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*req_context.stmt_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    dc.process_internal_events();
}

void flat_task::dag_schedule() {
    log_entry << *this;
    trace_scope_name("dag_schedule");  //NOLINT
    scheduler::dag_schedule(*req_context_);
    log_exit << *this;
}

void set_going_teardown_or_submit(
    request_context& req_context,
    bool try_on_suspended_worker
) {
    // note that this function can be called multiple times
    // once going_teardown is set to true, it should never go back to false.
    if(! global::config_pool()->inplace_teardown() || ! thread_local_info_.is_worker_thread()) {
        submit_teardown(req_context, try_on_suspended_worker);
        return;
    }
    auto& job = *req_context.job();
    auto completing = job.completing().load();
    if(completing) {
        // teardown task is scheduled or going_teardown is set
        return;
    }
    if(ready_to_finish(job, true)) {
        if (job.completing().compare_exchange_strong(completing, true)) {
            job.going_teardown().store(true);
            return;
        }
    }
    submit_teardown(req_context, try_on_suspended_worker);
}

bool check_or_submit_teardown(
    request_context& req_context,
    bool calling_from_task,
    bool try_on_suspended_worker
) {
    if(global::config_pool()->inplace_teardown()) {
        auto& job = *req_context.job();
        if(ready_to_finish(job, calling_from_task)) {
            auto completing = job.completing().load();
            if (! completing && job.completing().compare_exchange_strong(completing, true)) {
                return true;
            }
        }
    }
    submit_teardown(req_context, try_on_suspended_worker);
    return false;
}

void submit_teardown(request_context& req_context, bool try_on_suspended_worker) {
    // make sure teardown task is submitted only once
    auto& ts = *req_context.scheduler();
    auto& job = *req_context.job();
    auto completing = job.completing().load();
    if (! completing && job.completing().compare_exchange_strong(completing, true)) {
        ts.schedule_task(
            flat_task{task_enum_tag<flat_task_kind::teardown>, std::addressof(req_context)},
            schedule_option{
                try_on_suspended_worker ? schedule_policy_kind::suspended_worker : schedule_policy_kind::undefined
            }
        );
    }
}

void print_task_diagnostic(const flat_task &t, std::ostream &os) {
    os << std::boolalpha;
    os << "        - id: " << utils::hex(t.id()) << std::endl;
    os << "          kind: " << t.kind() << std::endl;
    os << "          sticky: " << t.sticky() << std::endl;
    if(t.req_context() && t.req_context()->job()) {
        os << "          job_id: " << utils::hex(t.req_context()->job()->id()) << std::endl;
    }
}

void flat_task::resubmit(request_context& req_context) {
    auto& ts = *req_context.scheduler();
    ts.schedule_task(flat_task{*this});
}

bool ready_to_finish(job_context& job, bool calling_from_task) {  //NOLINT(readability-make-member-function-const)
    // stop log_entry/log_exit since this function is called frequently as part of durability callback processing
    // log_entry << *this;
    trace_scope_name("teardown");  //NOLINT
    bool ret = true;
    std::size_t expected_task_count = calling_from_task ? 1 : 0;
    if (auto cnt = job.task_count().load(); cnt > expected_task_count) {
        VLOG_LP(log_debug) << job << " other " << cnt << " tasks remain and teardown is (re)scheduled";
        // Another teardown task will be scheduled at the end of this task.
        // It's not done here because newly scheduled task quickly completes and destroy job context.
        ret = false;
    } else if (job.completion_readiness() && !job.completion_readiness()()) {
        VLOG_LP(log_debug) << job << " job completion is not ready and teardown is (re)scheduled";
        ret = false;
    }
    // log_exit << *this << " ret:" << ret;
    return ret;
}

bool flat_task::write() {
    log_entry << *this;
    bool ret = false;
    if(utils::request_cancel_enabled(request_cancel_kind::write)) {
        auto& res_src = req_context_->req_info().response_source();
        if(res_src && res_src->check_cancel()) {
            cancel_request(*req_context_);
            if(check_or_submit_teardown(*req_context_, true)) {
                ret = true;
            };
            log_exit << *this;
            return ret;
        }
    }
    trace_scope_name("write");  //NOLINT
    (*write_)(*req_context_);
    if(check_or_submit_teardown(*req_context_, true)) {
        ret = true;
    };
    log_exit << *this;
    return ret;
}

bool flat_task::execute(tateyama::task_scheduler::context& ctx) {
    if (in_transaction_ && req_context_ && req_context_->transaction()) {
        auto& tctx = *req_context_->transaction();

        termination_state ts{};
        if (! tctx.termination_mgr().try_increment_task_use_count(ts)) {
            // set error info. for request context, and not for transaction context
            // if request context already has error info, it's not overwritten
            set_error(
                *req_context_,
                error_code::inactive_transaction_exception,
                "the other request already made to terminate the transaction",
                status::err_inactive_transaction
            );
            return check_or_submit_teardown(*req_context_, true, true);
        }
    }

    // The variables begin and end are needed only for timing event log.
    // Avoid calling clock::now() if log level is low. In case its cost is unexpectedly high.
    std::chrono::time_point<clock> begin{};
    if (VLOG_IS_ON(log_debug_timing_event_fine)) {
        begin = clock::now();
    }
    VLOG_LP(log_trace_fine) << "task begin " << *this << " job_id:" << utils::hex(req_context_->job()->id())
                            << " kind:" << kind_ << " sticky:" << sticky_ << " worker:" << ctx.index()
                            << " stolen:" << ctx.task_is_stolen() << " last_steal_from:" << ctx.last_steal_from();
    bool to_finish_job = false;
    switch(kind_) {
        using kind = flat_task_kind;
        case kind::dag_events: dag_schedule(); break;
        case kind::bootstrap: bootstrap(ctx); break;
        case kind::resolve: resolve(ctx); break;
        case kind::teardown: to_finish_job = ready_to_finish(*job(), false); break;
        case kind::wrapped: to_finish_job = execute_wrapped(); break;
        case kind::write: to_finish_job = write(); break;
        case kind::load: to_finish_job = load(); break;
    }
    std::chrono::time_point<clock> end{};
    std::size_t took_ns{};
    if (VLOG_IS_ON(log_debug_timing_event_fine)) {
        end = clock::now();
        took_ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end-begin).count();
    }
    if(auto req_detail = job()->request()) {
        req_detail->task_duration_ns() += took_ns;
        ++req_detail->task_count();
        if(sticky_) {
            ++req_detail->sticky_task_count();
        }
        if(ctx.task_is_stolen()) {
            ++req_detail->task_steling_count();
        }
    }
    VLOG_LP(log_trace_fine) << "task end " << *this << " took(ns):" << took_ns
                            << " job_id:" << utils::hex(req_context_->job()->id()) << " kind:" << kind_
                            << " sticky:" << sticky_ << " worker:" << ctx.index() << " stolen:" << ctx.task_is_stolen();

    if (in_transaction_ && req_context_ && req_context_->transaction()) {
        auto& tctx = *req_context_->transaction();
        termination_state ts{};
        tctx.termination_mgr().decrement_task_use_count(ts);
        if (ts.going_to_abort() && ts.task_empty()) {
            (void) req_context_->transaction()->abort_transaction();
            // request_info in the request context might not be the cause of the abort
            // rather it is the request processing SQL that is interrupted by the abort
            // TODO fix if that is a problem.
            log_end_of_tx(*req_context_->transaction(), true, req_context_->req_info());
        }
    }
    return to_finish_job;
}

void finish_job(request_context& req_context) {
    // job completed, and the latch needs to be released
    auto& ts = *req_context.scheduler();
    auto& j = *req_context.job();
    auto& cb = j.callback();
    auto req_detail = j.request();
    if(cb) {
        cb();
    }
    VLOG_LP(log_trace_fine) << "job teardown job_id:" << utils::hex(req_context.job()->id());
    if(req_detail) {
        req_detail->status(scheduler::request_detail_status::finishing);
        log_request(*req_detail, req_context.status_code() == status::ok);

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

void flat_task::operator()(tateyama::task_scheduler::context& ctx) {
    auto started = job()->started().load();
    if(! started) {
        if(job()->started().compare_exchange_strong(started, true)) {
            if(auto req = job()->request()) {
                req->status(scheduler::request_detail_status::executing);
                log_request(*req);
            }
        }
    }
    auto& tctx = req_context_->transaction();
    bool job_completes{};
    {
        auto lk = (tctx && sticky_) ?
            std::unique_lock{tctx->mutex()} :
            std::unique_lock<transaction_context::mutex_type>{};
        job_completes = execute(ctx) || job()->going_teardown();
        if (tctx && sticky_) {
            tctx->decrement_worker_count();
        }
    }
    if(kind_ != flat_task_kind::teardown) {
        // teardown tasks should not be counted
        auto jobid = job()->id();
        auto cnt = --job()->task_count();
        // Be careful and don't touch job or request contexts after decrementing the counter which makes teardown to finish job.
        (void)cnt;
        (void)jobid;
        //VLOG_LP(log_debug) << "decremented job " << jobid << " task count to " << cnt;
        if(! job_completes) {
            return;
        }
    }

    // teardown task or job_completes=true
    if(! job_completes) {
        // teardown task is not ready to finish_job

        // Submitting teardown should be done at the end of the task since otherwise new teardown finish fast
        // and start destroying job context, which can be touched by this task.
        resubmit(*req_context_);
        return;
    }
    finish_job(*req_context_);
}

flat_task::identity_type flat_task::id() const {
    if (origin_) {
        return origin_->id();
    }
    return id_;
}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::wrapped>,
    request_context* rctx,
    std::shared_ptr<model::task> origin
) noexcept:
    kind_(flat_task_kind::wrapped),
    req_context_(rctx),
    origin_(std::move(origin)),
    sticky_(origin_->transaction_capability() == model::task_transaction_kind::sticky),
    in_transaction_(origin_->transaction_capability() != model::task_transaction_kind::none)
{}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::dag_events>,
    request_context* rctx
) noexcept:
    id_(id_src_++),
    kind_(flat_task_kind::dag_events),
    req_context_(rctx)
{}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::bootstrap>,
    request_context* rctx,
    model::graph& g
) noexcept:
    id_(id_src_++),
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

void flat_task::resolve(tateyama::task_scheduler::context& ctx) {
    log_entry << *this;
    (void)ctx;
    auto& e = sctx_->executable_statement_;
    if(auto res = sctx_->database_->resolve(sctx_->prepared_,
            maybe_shared_ptr{sctx_->parameters_}, e); res != status::ok) {
        set_error(
            *req_context_,
            error_code::sql_execution_exception,
            string_builder{} << "creating parallel execution plan failed. status:" << res << string_builder::to_string,
            res
        );
    } else {
        executor::execute_async_on_context(
                *sctx_->database_,
                req_context_.ownership(),
                maybe_shared_ptr{e.get()},
                [sctx=sctx_](
                    status st,
                    std::shared_ptr<error::error_info> info,
                    std::shared_ptr<request_statistics> stats
                ) { // pass sctx_ to live long enough
                    sctx->callback_(st, std::move(info), std::move(stats));
                },
                false,
                req_context_->req_info()
        );
    }
    log_exit << *this;
}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::teardown>,
    request_context* rctx
) noexcept:
    id_(id_src_++),
    kind_(flat_task_kind::teardown),
    req_context_(rctx)
{}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::write>,
    request_context* rctx,
    executor::common::write_statement* write
) noexcept:
    id_(id_src_++),
    kind_(flat_task_kind::write),
    req_context_(rctx),
    write_(write),
    sticky_(true)
{}

bool flat_task::sticky() const noexcept {
    return sticky_;
}

bool flat_task::in_transaction() const noexcept {
    return in_transaction_;
}

request_context* flat_task::req_context() const noexcept {
    return req_context_.get();
}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::resolve>,
    std::shared_ptr<request_context> rctx,
    std::shared_ptr<statement_context> sctx
) noexcept:
    id_(id_src_++),
    kind_(flat_task_kind::resolve),
    req_context_(std::move(rctx)),
    sctx_(std::move(sctx))
{}

bool flat_task::load() {
    log_entry << *this;
    trace_scope_name("load");  //NOLINT
    auto res = (*loader_)();
    bool ret = false;
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
            set_error(
                *req_context_,
                error_code::sql_execution_exception,
                msg,
                st
            );
        }
        submit_teardown(*req_context_);
    }
    log_exit << *this;
    return ret;
}

flat_task::flat_task(task_enum_tag_t<flat_task_kind::load>, request_context* rctx,
    std::shared_ptr<executor::file::loader> ldr) noexcept:
    id_(id_src_++),
    kind_(flat_task_kind::load),
    req_context_(rctx),
    loader_(std::move(ldr))
{}

bool flat_task::execute_wrapped() {
    //DVLOG(log_trace) << *this << " wrapped task executed.";
    trace_scope_name("executor_task");  //NOLINT
    model::task_result res{};
    while((res = (*origin_)()) == model::task_result::proceed) {}
    if(res == model::task_result::yield) {
        resubmit(*req_context_);
        return false;
    }
    return res == model::task_result::complete_and_teardown;
}

}  // namespace jogasaki::scheduler
