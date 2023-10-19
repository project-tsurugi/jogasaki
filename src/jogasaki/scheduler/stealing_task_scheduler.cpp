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
#include "stealing_task_scheduler.h"

#include <glog/logging.h>
#include <takatori/util/exception.h>

#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/utils/hex.h>
#include "task_scheduler.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

using takatori::util::throw_exception;

stealing_task_scheduler::stealing_task_scheduler(thread_params params) :
    scheduler_cfg_(create_scheduler_cfg(params)),
    scheduler_(scheduler_cfg_)
{}

std::size_t determine_worker(transaction_context const& tx, std::size_t worker_count) {
    return tx.id() % worker_count;  //NOLINT
}

void stealing_task_scheduler::do_schedule_conditional_task(conditional_task &&t) {
    trace_scope_name("do_schedule_conditional_task");  //NOLINT
    scheduler_.schedule_conditional(std::move(t));
}

void stealing_task_scheduler::do_schedule_task(flat_task&& t, schedule_option opt) {
    trace_scope_name("do_schedule_task");  //NOLINT
    auto& rctx = *t.req_context();
    auto& jctx = *rctx.job();
    auto idx = jctx.preferred_worker_index().load();
    if(auto& tctx = rctx.transaction(); tctx && t.sticky()) {
        std::uint32_t worker(
            idx != job_context::undefined_index ? idx :
                (scheduler_cfg_.use_preferred_worker_for_current_thread() ?
                    scheduler_.preferred_worker_for_current_thread() :
                    scheduler_.next_worker()
                )
        );
        auto candidate = worker;
        while(true) {
            if(tctx->increment_worker_count(worker)) {
                if(worker != candidate) {
                    // The tx is already in use and task is assigned to different worker than original candidate.
                    if(auto req_detail = t.job()->request()) {
                        ++req_detail->sticky_task_worker_enforced_count();
                    }
                }
                scheduler_.schedule_at(std::move(t), worker);
                return;
            }
            // Other task is already scheduled to use the tx.
            // Continue loop and schedule at the same worker as existing task.
        }
    }
    if (opt.policy() == schedule_policy_kind::suspended_worker) {
        // scheduling policy is effective only for non-sticky task
        scheduler_.schedule(std::move(t), convert(opt));
        return;
    }
    if (idx != job_context::undefined_index) {
        scheduler_.schedule_at(std::move(t), idx);
        return;
    }
    scheduler_.schedule(std::move(t), convert(opt));
}

void stealing_task_scheduler::wait_for_progress(std::size_t id) {
    DVLOG(log_trace) << "wait_for_progress begin";
    if (id == job_context::undefined_id) {
        // this case is for testing purpose
        // empty() is not thread safe or 100% accurate under concurrency modification
        while(! job_contexts_.empty()) {
            _mm_pause();
        }
        return;
    }

    std::shared_ptr<job_context> holder{};
    {
        decltype(job_contexts_)::accessor acc{};
        if (! job_contexts_.find(acc, id)) {
            // job already completed and is erased for this scheduler. Nothing to wait.
            return;
        }
        holder = acc->second;
    }
    holder->completion_latch().wait();
    DVLOG(log_trace) << "wait_for_progress completed";
}

void stealing_task_scheduler::start() {
    scheduler_.start();
}

void stealing_task_scheduler::stop() {
    scheduler_.stop();
    scheduler_.print_worker_stats(
        LOG(INFO) << "/:jogasaki:scheduler:stealing_task_scheduler:stop Task scheduler statistics "
    );

    // Following shared_ptr cycle can exist and un-finished job causes memory leak after stopping database:
    // request_context -> task_scheduler -> job_context -> job completion callback -> request_context
    // To avoid this clear job contexts even if they are unfinished.
    job_contexts_.clear(); // to avoid memory leak
}

task_scheduler_kind stealing_task_scheduler::kind() const noexcept {
    return task_scheduler_kind::stealing;
}

tateyama::task_scheduler::task_scheduler_cfg stealing_task_scheduler::create_scheduler_cfg(thread_params params) {
    tateyama::task_scheduler::task_scheduler_cfg ret{};
    ret.thread_count(params.threads());
    ret.force_numa_node(params.force_numa_node());
    ret.core_affinity(params.is_set_core_affinity());
    ret.assign_numa_nodes_uniformly(params.assign_numa_nodes_uniformly());
    ret.initial_core(params.inititial_core());
    ret.stealing_enabled(params.stealing_enabled());
    ret.use_preferred_worker_for_current_thread(params.use_preferred_worker_for_current_thread());
    ret.stealing_wait(params.stealing_wait());
    ret.task_polling_wait(params.task_polling_wait());
    ret.busy_worker(params.busy_worker());
    ret.watcher_interval(params.watcher_interval());
    ret.worker_try_count(params.worker_try_count());
    ret.worker_suspend_timeout(params.worker_suspend_timeout());
    return ret;
}

void stealing_task_scheduler::register_job(std::shared_ptr<job_context> ctx) {
    auto cid = ctx->id();
    if(! job_contexts_.emplace(cid, std::move(ctx))) {
        throw_exception(std::logic_error{""});
    }
}

void stealing_task_scheduler::unregister_job(std::size_t job_id) {
    if(! job_contexts_.erase(job_id)) {
        throw_exception(std::logic_error{""});
    }
}

void stealing_task_scheduler::print_diagnostic(std::ostream &os) {
    // In order to avoid timing issue while printing jobs,
    // copy job contexts first so that it keeps shared_ptr until end of printing.
    // Iterating tbb hash map is not thread-safe, and this is not complete solution,
    // but we are doing our best for safety as much as possible.
    std::unordered_map<std::size_t, std::shared_ptr<job_context>> copied{};
    for(auto [k, c] : job_contexts_) {
        (void) c;
        {
            decltype(job_contexts_)::accessor acc{};
            if (! job_contexts_.find(acc, k)) {
                // job already completed and is erased
                continue;
            }
            copied.emplace(k, acc->second);
        }
    }
    auto jobs = copied.size();
    os << "job_count: " << jobs << std::endl;
    if(jobs > 0) {
        os << "jobs:" << std::endl;
        for(auto&& [k, ctx] : copied) {
            os << "  - job_id: " << utils::hex(ctx->id()) << std::endl;
            if(auto diag = ctx->request()) {
                os << "    job_kind: " << diag->kind() << std::endl;
                os << "    job_status: " << diag->status() << std::endl;
                os << "    sql_text: " << diag->statement_text() << std::endl;
                os << "    transaction_id: " << diag->transaction_id() << std::endl;
                os << "    channel_status: " << diag->channel_status() << std::endl;
                os << "    channel_name: " << diag->channel_name() << std::endl;
            }
            os << "    task_count: " << ctx->task_count() << std::endl;
        }
    }
    scheduler_.print_diagnostic(os);
}
}



