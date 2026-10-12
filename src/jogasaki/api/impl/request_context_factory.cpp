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
#include <jogasaki/executor/io/writer_pool.h>
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
    bool has_result_records,
    std::shared_ptr<scheduler::request_detail> request_detail,
    scheduler::task_scheduler* scheduler_override
) {
    auto& c = db.configuration();
    // request, statement and job registration must refer to the same scheduler
    auto& ts = scheduler_override ? *scheduler_override : *db.task_scheduler();
    auto rctx = std::make_shared<request_context>(
        c,
        std::move(resource),
        db.kvs_db(),
        std::move(tx),
        db.sequence_manager(),
        channel
    );
    rctx->req_info(req_info);
    if(scheduler_override) {
        rctx->scheduler(maybe_shared_ptr<scheduler::task_scheduler>{scheduler_override});
    } else {
        rctx->scheduler(db.scheduler());
    }
    rctx->stmt_scheduler(
        std::make_shared<scheduler::statement_scheduler>(
            db.configuration(),
            ts
        )
    );
    rctx->storage_provider(db.tables());
    rctx->remote_catalogs(db.remote_catalogs());
    rctx->remote_catalogs_manager(db.remote_catalogs_manager());
    rctx->remote_server_channels(std::addressof(db.remote_server_channels()));

    // Initialize writer_pool if record_channel is provided for query
    if(channel && has_result_records) {
        // request has emit operator, though the channel can possibly be null_record_channel
        // (discard query result since api for statement is used)

        // If channel has no max limit, we use max_result_set_writers as the number of seats.
        // Though max_result_set_writers is large and expensive, this case is either null record channel or result
        // set channel, and performance is not important.
        std::size_t max_writers =
            channel->max_writer_count().has_value() ? channel->max_writer_count().value() : c->max_result_set_writers();
        rctx->writer_pool(std::make_shared<executor::io::writer_pool>(*channel, max_writers));
    }

    if (request_detail && req_info.request_source()) {
        request_detail->local_id(req_info.request_source()->local_id());
        request_detail->session_id(req_info.request_source()->session_id());
    }

    auto job = std::make_shared<scheduler::job_context>();
    job->request(std::move(request_detail));

    rctx->job(maybe_shared_ptr{job.get()});

    ts.register_job(job);
    return rctx;
}

}
