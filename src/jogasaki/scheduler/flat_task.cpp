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

#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/impl/executable_statement.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <jogasaki/utils/core_affinity.h>
#include <tateyama/api/task_scheduler/context.h>
#include <jogasaki/executor/common/execute.h>

namespace jogasaki::scheduler {

using takatori::util::fail;
using takatori::util::unsafe_downcast;

void flat_task::bootstrap(tateyama::api::task_scheduler::context& ctx) {
    DVLOG(1) << *this << " bootstrap task executed.";
    trace_scope_name("bootstrap");  //NOLINT
    job_context_->index().store(ctx.index());
    auto& sc = scheduler::statement_scheduler::impl::get_impl(*job_context_->dag_scheduler());
    auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
    if (dc.cfg().respect_client_core()) {
        jogasaki::utils::thread_core_affinity(job_context_->invoker_thread_cpu_id());
    }
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
    job_context_->completion_latch().release();
    if (auto& cb = job_context_->callback(); cb) {
        cb();
    }
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
        case kind::bootstrap_resolving: bootstrap_resolving(ctx); return true;
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

void flat_task::resolve(tateyama::api::task_scheduler::context& ctx) {
    (void)ctx;
    auto& e = *executable_statement_container_;
    if(auto res = database_->resolve(*prepared_, *parameters_, e); res != status::ok) {
        fail();
    }
    auto&s = unsafe_downcast<api::impl::executable_statement&>(*e);
    auto* stmt = unsafe_downcast<executor::common::execute>(
        s.body()->operators().get()
    );
    auto& g = stmt->operators();
    graph_ = std::addressof(g);

    auto& cfg = database_->configuration();
    *result_store_container_ = std::make_unique<data::result_store>();
    auto& request_ctx = *request_context_container_;
    request_ctx = std::make_shared<request_context>(
        cfg,
        s.resource(),
        database_->kvs_db(),
        tx_,
        database_->sequence_manager(),
        (*result_store_container_).get()
    );
    g.context(*request_ctx);
    request_ctx->job(maybe_shared_ptr<job_context>{job_context_});
}

void flat_task::bootstrap_resolving(tateyama::api::task_scheduler::context& ctx) {
    resolve(ctx);
    bootstrap(ctx);
}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::teardown>,
    job_context* jctx
) noexcept:
    kind_(flat_task_kind::teardown),
    job_context_(jctx)
{}

flat_task::flat_task(
    task_enum_tag_t<flat_task_kind::bootstrap_resolving>,
    job_context* jctx,
    api::prepared_statement const& prepared,
    api::parameter_set const& parameters,
    api::impl::database& database,
    std::shared_ptr<kvs::transaction> tx,
    std::shared_ptr<request_context>& rctx,
    std::unique_ptr<data::result_store>& result_store_container,
    std::unique_ptr<api::executable_statement>& executable_statement_container
) noexcept:
    kind_(flat_task_kind::bootstrap_resolving),
    job_context_(jctx),
    prepared_(std::addressof(prepared)),
    parameters_(std::addressof(parameters)),
    database_(std::addressof(database)),
    tx_(std::move(tx)),
    request_context_container_(std::addressof(rctx)),
    result_store_container_(std::addressof(result_store_container)),
    executable_statement_container_(std::addressof(executable_statement_container))
{}

}



