/*
 * Copyright 2018-2025 Project Tsurugi.
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
#include "request_context_factory.h"

#include <utility>

#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/memory/lifo_paged_memory_resource.h>
#include <jogasaki/request_context.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/statement_scheduler.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/transaction_context.h>

namespace jogasaki::api::impl {

std::shared_ptr<request_context> create_request_context(
    impl::database& db,
    std::shared_ptr<transaction_context> tx,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    std::shared_ptr<memory::lifo_paged_memory_resource> resource,
    request_info const& req_info,
    std::shared_ptr<scheduler::request_detail> request_detail
) {
    auto& c = db.configuration();
    auto rctx = std::make_shared<request_context>(
        c,
        std::move(resource),
        db.kvs_db(),
        std::move(tx),
        db.sequence_manager(),
        channel
    );
    rctx->req_info(req_info);
    rctx->scheduler(db.scheduler());
    rctx->stmt_scheduler(
        std::make_shared<scheduler::statement_scheduler>(
            db.configuration(),
            *db.task_scheduler()
        )
    );
    rctx->storage_provider(db.tables());

    if (request_detail && req_info.request_source()) {
        request_detail->local_id(req_info.request_source()->local_id());
        request_detail->session_id(req_info.request_source()->session_id());
    }

    auto job = std::make_shared<scheduler::job_context>();
    job->request(std::move(request_detail));

    rctx->job(maybe_shared_ptr{job.get()});

    auto& ts = *db.task_scheduler();
    ts.register_job(job);
    return rctx;
}

}
