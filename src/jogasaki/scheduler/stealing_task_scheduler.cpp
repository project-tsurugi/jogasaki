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
#include "details/thread_pool.h"
#include "task_scheduler.h"
#include "thread_params.h"

namespace jogasaki::scheduler {

void flat_task::operator()(tateyama::context& ctx) {
    (void)ctx;
    if (dag_scheduling_) {
        auto& sc = scheduler::statement_scheduler::impl::get_impl(*request_context_->dag_scheduler());
        auto& dc = scheduler::dag_controller::impl::get_impl(sc.controller());
        dc.process(false);
        return;
    }
    auto res = (*origin_)();
    (void)res;
}

stealing_task_scheduler::stealing_task_scheduler(thread_params params) :
    scheduler_cfg_(create_scheduler_cfg(params)),
    scheduler_(scheduler_cfg_)
{}

void stealing_task_scheduler::schedule_task(std::shared_ptr<model::task> const& task) {
    scheduler_.schedule(flat_task{task});
}

void stealing_task_scheduler::schedule_flat_task(flat_task task) {
    scheduler_.schedule(std::move(task));
}

void stealing_task_scheduler::wait_for_progress() {
    // do nothing
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

tateyama::task_scheduler_cfg stealing_task_scheduler::create_scheduler_cfg(thread_params params) {
    tateyama::task_scheduler_cfg ret{};
    ret.thread_count(params.threads());
    ret.force_numa_node(params.force_numa_node());
    ret.core_affinity(params.is_set_core_affinity());
    ret.assign_numa_nodes_uniformly(params.assign_numa_nodes_uniformly());
    ret.initial_core(params.inititial_core());
    ret.stealing_enabled(params.stealing_enabled());
    return ret;
}
}



