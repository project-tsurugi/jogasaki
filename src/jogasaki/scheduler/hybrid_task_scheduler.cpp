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
#include "hybrid_task_scheduler.h"

namespace jogasaki::scheduler {

hybrid_task_scheduler::hybrid_task_scheduler(thread_params params) :
    stealing_scheduler_(std::move(params))
{}

void hybrid_task_scheduler::do_schedule_task(flat_task&& t) {
    auto* rctx = t.req_context();
    if(rctx->lightweight()) {
        auto& mode = t.job()->hybrid_execution_mode();
        while(true) {
            auto cur = mode.load();
            if(cur == hybrid_execution_mode_kind::serial) {
                serial_scheduler_.do_schedule_task(std::move(t));
                return;
            }
            if(cur == hybrid_execution_mode_kind::undefined) {
                // TODO check if tx lock available, and then acquire the lock
                if(! mode.compare_exchange_strong(cur, hybrid_execution_mode_kind::serial)) {
                    continue;
                }
                auto jobid = t.job()->id();
                serial_scheduler_.do_schedule_task(std::move(t));
                serial_scheduler_.wait_for_progress(jobid);
                // TODO unlock
                return;
            }
        }
    }
    stealing_scheduler_.do_schedule_task(std::move(t));
}

void hybrid_task_scheduler::start() {
    stealing_scheduler_.start();
}

void hybrid_task_scheduler::stop() {
    stealing_scheduler_.stop();
}

task_scheduler_kind hybrid_task_scheduler::kind() const noexcept {
    return task_scheduler_kind::hybrid;
}

void hybrid_task_scheduler::register_job(std::shared_ptr<job_context> ctx) {
    // even if the job runs on serial scheduler, its id is managed by the stealing scheduler
    stealing_scheduler_.register_job(std::move(ctx));
}

void hybrid_task_scheduler::unregister_job(std::size_t job_id) {
    stealing_scheduler_.unregister_job(job_id);
}

void hybrid_task_scheduler::print_diagnostic(std::ostream &os) {
    stealing_scheduler_.print_diagnostic(os);
}

void hybrid_task_scheduler::wait_for_progress(std::size_t id) {
    stealing_scheduler_.wait_for_progress(id);
}

}



