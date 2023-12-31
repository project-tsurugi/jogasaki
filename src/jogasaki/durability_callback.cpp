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
#include "durability_callback.h"

#include <atomic>
#include <memory>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/utils/use_counter.h>

namespace jogasaki {

void durability_callback::operator()(durability_callback::marker_type marker) {
    // Avoid tracing entry. This function is called frequently. Trace only effective calls below.
    [[maybe_unused]] auto cnt = db_->requests_inprocess();
    if(db_->stop_requested()) return;
    commit_profile::time_point durability_callback_invoked{};
    if(db_->config()->profile_commits()) {
        durability_callback_invoked = commit_profile::clock::now();
    }
    if(manager_->instant_update_if_waitlist_empty(marker)) {
        // wait-list is empty and marker is updated quickly
        return;
    }
    auto request_ctx = api::impl::create_request_context(
        *db_,
        nullptr,
        nullptr,
        nullptr,
        std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::process_durability_callback)
    );
    request_ctx->job()->callback([request_ctx](){
        (void) request_ctx;
    });

    scheduler_->schedule_task(
        scheduler::create_custom_task(
            request_ctx.get(),
            [mgr=manager_, marker, request_ctx=request_ctx.get(), durability_callback_invoked](){ // capture request_ctx pointer to avoid cyclic dependency
                if(mgr->update_current_marker(
                    marker,
                    [marker, durability_callback_invoked](element_reference_type e){
                        VLOG(log_trace) << "/:jogasaki:durability_callback:operator() "
                            << "--- current:" << marker << " txid:" << e->transaction()->transaction_id() << " marker:" << *e->transaction()->durability_marker();
                        e->transaction()->profile()->set_durability_cb_invoked(durability_callback_invoked);
                        scheduler::submit_teardown(*e, false, true);
                    })) {
                    scheduler::submit_teardown(*request_ctx);
                    return model::task_result::complete;
                }
                return model::task_result::yield;
            },
            false,
            false
        ),
        scheduler::schedule_option{scheduler::schedule_policy_kind::suspended_worker}
    );
}

durability_callback::durability_callback(
    api::impl::database& db
) :
    db_(std::addressof(db)),
    manager_(db.durable_manager().get()),
    scheduler_(db.task_scheduler())
{}
}

