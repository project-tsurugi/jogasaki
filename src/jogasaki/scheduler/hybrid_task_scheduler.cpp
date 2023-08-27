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
    stealing_scheduler_(params)
{}

void hybrid_task_scheduler::do_schedule_conditional_task(conditional_task&& t) {  //NOLINT(readability-function-cognitive-complexity)
    stealing_scheduler_.do_schedule_conditional_task(std::move(t));
}

void hybrid_task_scheduler::do_schedule_task(flat_task&& t, schedule_option opt) {  //NOLINT(readability-function-cognitive-complexity)
    auto& mode = t.job()->hybrid_execution_mode();
    auto* rctx = t.req_context();
    while(true) {  // retry from here if modifying `mode` variable fails
        auto cur = mode.load();
        if(cur == hybrid_execution_mode_kind::serial) {
            serial_scheduler_.do_schedule_task(std::move(t), opt);
            return;
        }
        if(cur == hybrid_execution_mode_kind::stealing) {
            stealing_scheduler_.do_schedule_task(std::move(t), opt);
            return;
        }

        // cur == hybrid_execution_mode_kind::undefined
        if(! rctx->lightweight()) {
            if (! mode.compare_exchange_strong(cur, hybrid_execution_mode_kind::stealing)) {
                continue;
            }
            if(auto d = t.job()->request()) {
                d->hybrid_execution_mode(hybrid_execution_mode_kind::stealing);
            }
        }
        auto& tx = t.req_context()->transaction();
        {
            auto lk = tx ?
                std::unique_lock{tx->mutex(), std::defer_lock} :
                std::unique_lock<transaction_context::mutex_type>{};
            if (tx && ! lk.try_lock()) {
                if (! mode.compare_exchange_strong(cur, hybrid_execution_mode_kind::stealing)) {
                    continue;
                }
                if(auto d = t.job()->request()) {
                    d->hybrid_execution_mode(hybrid_execution_mode_kind::stealing);
                }
                stealing_scheduler_.do_schedule_task(std::move(t), opt);
                return;
            }

            // without tx, or tx lock acquired successfully
            if (! mode.compare_exchange_strong(cur, hybrid_execution_mode_kind::serial)) {
                continue;
            }
            if(auto d = t.job()->request()) {
                d->hybrid_execution_mode(hybrid_execution_mode_kind::serial);
            }
            auto jobid = t.job()->id();
            serial_scheduler_.do_schedule_task(std::move(t), opt);
            serial_scheduler_.wait_for_progress(jobid);
        }
        break;
    }
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



