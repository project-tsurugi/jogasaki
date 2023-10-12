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
#include "request_context_factory.h"

#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/statement_scheduler.h>

namespace jogasaki::api::impl {

std::shared_ptr<request_context> create_request_context(
    impl::database& db,
    std::shared_ptr<transaction_context> tx,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    std::shared_ptr<memory::lifo_paged_memory_resource> resource,
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
    rctx->scheduler(db.scheduler());
    rctx->stmt_scheduler(
        std::make_shared<scheduler::statement_scheduler>(
            db.configuration(),
            *db.task_scheduler()
        )
    );
    rctx->storage_provider(db.tables());

    auto job = std::make_shared<scheduler::job_context>();
    job->request(std::move(request_detail));
    rctx->job(maybe_shared_ptr{job.get()});

    auto& ts = *db.task_scheduler();
    ts.register_job(job);
    return rctx;
}

}
