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
#include "stealing_task_scheduler.h"

#include <jogasaki/model/task.h>
#include <jogasaki/scheduler/statement_scheduler_impl.h>
#include <jogasaki/scheduler/dag_controller_impl.h>
#include <jogasaki/scheduler/job_context.h>
#include "task_scheduler.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

stealing_task_scheduler::stealing_task_scheduler(thread_params params) :
    scheduler_cfg_(create_scheduler_cfg(params)),
    scheduler_(scheduler_cfg_)
{}

void stealing_task_scheduler::do_schedule_task(flat_task&& t) {
    auto& jctx = *t.job(); // TODO for now scheduling job tasks into the same thread
    auto idx = jctx.index().load();
    if (idx == job_context::undefined_index) {
        scheduler_.schedule(std::move(t));
        return;
    }
    scheduler_.schedule_at(std::move(t), idx);
}

void stealing_task_scheduler::wait_for_progress(job_context& ctx) {
    DVLOG(1) << "wait_for_progress begin";
    // if callback is set, asynchronous call is in-progress. So we don't need to wait.
    if (! ctx.callback()) {
        ctx.completion_latch().wait();
    }
    DVLOG(1) << "wait_for_progress completed";
}

void stealing_task_scheduler::start() {
    scheduler_.start();
}

void stealing_task_scheduler::stop() {
    scheduler_.stop();
}

task_scheduler_kind stealing_task_scheduler::kind() const noexcept {
    return task_scheduler_kind::stealing;
}

tateyama::api::task_scheduler::task_scheduler_cfg stealing_task_scheduler::create_scheduler_cfg(thread_params params) {
    tateyama::api::task_scheduler::task_scheduler_cfg ret{};
    ret.thread_count(params.threads());
    ret.force_numa_node(params.force_numa_node());
    ret.core_affinity(params.is_set_core_affinity());
    ret.assign_numa_nodes_uniformly(params.assign_numa_nodes_uniformly());
    ret.initial_core(params.inititial_core());
    ret.stealing_enabled(params.stealing_enabled());
    return ret;
}
}



