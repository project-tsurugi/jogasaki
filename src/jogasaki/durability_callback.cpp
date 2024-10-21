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

#include <memory>
#include <optional>
#include <ostream>
#include <string_view>
#include <type_traits>
#include <utility>
#include <glog/logging.h>

#include <takatori/util/maybe_shared_ptr.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/commit_profile.h>
#include <jogasaki/configuration.h>
#include <jogasaki/durability_common.h>
#include <jogasaki/durability_manager.h>
#include <jogasaki/logging.h>
#include <jogasaki/model/task.h>
#include <jogasaki/request_context.h>
#include <jogasaki/request_logging.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/scheduler/schedule_option.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/set_cancel_status.h>

namespace jogasaki {

void durability_callback::operator()(durability_callback::marker_type marker) {
    // Avoid tracing entry. This function is called frequently. Trace only effective calls below.
    [[maybe_unused]] auto cnt = db_->requests_inprocess();
    if(db_->stop_requested()) return;
    if(db_->config()->omit_task_when_idle() && manager_->instant_update_if_waitlist_empty(marker)) {
        // wait-list is empty and marker is updated quickly
        return;
    }
    commit_profile::time_point durability_callback_invoked{};
    if(db_->config()->profile_commits()) {
        durability_callback_invoked = commit_profile::clock::now();
    }

    auto req_detail =
        std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::process_durability_callback);
    req_detail->status(scheduler::request_detail_status::accepted);
    log_request(*req_detail);

    auto request_ctx = api::impl::create_request_context(
        *db_,
        nullptr,
        nullptr,
        nullptr,
        {},
        std::move(req_detail)
    );
    request_ctx->job()->callback([request_ctx](){
        (void) request_ctx;
    });

    scheduler_->schedule_task(
        scheduler::create_custom_task(
            request_ctx.get(),
            [mgr=manager_, marker, request_ctx=request_ctx.get(), durability_callback_invoked](){ // capture request_ctx pointer to avoid cyclic dependency
                mgr->check_cancel(
                    [marker](element_reference_type e){
                        VLOG(log_trace) << "/:jogasaki:durability_callback:operator() check_cancel "
                            << "--- current:" << marker << " txid:" << e->transaction()->transaction_id() << " marker:" << *e->transaction()->durability_marker();
                        set_cancel_status(*e);
                        submit_commit_response(e, commit_response_kind::stored, true, true);
                    }
                );
                if(mgr->update_current_marker(
                    marker,
                    [marker, durability_callback_invoked, request_ctx](element_reference_type e){
                        VLOG(log_trace) << "/:jogasaki:durability_callback:operator() "
                            << "--- current:" << marker << " txid:" << e->transaction()->transaction_id() << " marker:" << *e->transaction()->durability_marker();
                        request_ctx->job()->request()->affected_txs().add(e->transaction()->transaction_id());
                        e->transaction()->profile()->set_durability_cb_invoked(durability_callback_invoked);
                        submit_commit_response(e, commit_response_kind::stored, false, true);
                    })) {
                    scheduler::submit_teardown(*request_ctx);
                    return model::task_result::complete;
                }
                return model::task_result::yield;
            },
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

}  // namespace jogasaki
