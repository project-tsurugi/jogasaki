/*
 * Copyright 2018-2022 tsurugi project.
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

#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/utils/use_counter.h>

namespace jogasaki {

void durability_callback::operator()(durability_callback::marker_type marker) {
    [[maybe_unused]] auto cnt = db_->requests_inprocess();
    if(db_->stop_requested()) return;
    auto request_ctx = api::impl::create_request_context(db_, nullptr, nullptr, nullptr);
    request_ctx->job()->callback([request_ctx](){
        (void) request_ctx;
    });
    scheduler_->schedule_task(
        scheduler::create_custom_task(
            request_ctx.get(),
            [mgr=manager_, marker, request_ctx=request_ctx.get()](){ // capture request_ctx pointer to avoid cyclic dependency
                if(mgr->update_current_marker(
                    marker,
                    [](element_reference_type e){
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

