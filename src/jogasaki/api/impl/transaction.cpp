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
#include "transaction.h"
#include "executable_statement.h"

#include <takatori/util/downcast.h>

#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/executor/sequence/sequence.h>

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;

status transaction::commit() {
    return tx_->commit();
}

status transaction::abort() {
    return tx_->abort();
}

status transaction::execute(api::executable_statement& statement) {
    std::unique_ptr<api::result_set> result{};
    return execute(statement, result);
}

status transaction::execute(
    api::executable_statement& statement,
    std::unique_ptr<api::result_set>& result
) {
    auto& s = unsafe_downcast<impl::executable_statement&>(statement);
    auto& e = s.body();
    auto& c = database_->configuration();
    auto store = std::make_unique<data::result_store>();
    auto request_ctx = std::make_shared<request_context>(
        c,
        s.resource(),
        database_->kvs_db(),
        tx_,
        database_->sequence_manager(),
        store.get()
    );
    if (e->is_execute()) {
        auto* stmt = unsafe_downcast<executor::common::execute>(e->operators().get());
        auto& g = stmt->operators();
        g.context(*request_ctx);
        std::size_t cpu = sched_getcpu();
        request_ctx->job(
            std::make_shared<scheduler::job_context>(
                maybe_shared_ptr{std::addressof(scheduler_)},
                cpu
            )
        );

        auto& ts = scheduler_.get_task_scheduler();
        ts.schedule_task(scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::bootstrap>,
            request_ctx->job().get(),
            g
        });
        ts.wait_for_progress(*request_ctx->job());

        // for now, assume only one result is returned
        result = std::make_unique<impl::result_set>(
            std::move(store)
        );
        return request_ctx->status_code();
    }
    auto* stmt = unsafe_downcast<executor::common::write>(e->operators().get());
    scheduler_.schedule(*stmt, *request_ctx);
    return request_ctx->status_code();
}

impl::database& transaction::database() {
    return *database_;
}

transaction::transaction(
    impl::database& database,
    bool readonly
) :
    database_(std::addressof(database)),
    scheduler_(database_->configuration(), *database_->task_scheduler()),
    tx_(database_->kvs_db()->create_transaction(readonly))
{}

status transaction::execute(
    api::prepared_statement const& prepared,
    api::parameter_set const& parameters
) {
    std::unique_ptr<api::result_set> result{};
    return execute(prepared, parameters, result);
}

status transaction::execute(
    api::prepared_statement const& prepared,
    api::parameter_set const& parameters,
    std::unique_ptr<api::result_set>& result
) {
    auto& ts = scheduler_.get_task_scheduler();
    auto job = std::make_shared<scheduler::job_context>();
    job->dag_scheduler(maybe_shared_ptr{std::addressof(scheduler_)});
    std::shared_ptr<request_context> request_ctx{};
    std::unique_ptr<data::result_store> store{};
    std::unique_ptr<api::executable_statement> exec{};
    ts.schedule_task(scheduler::flat_task{
        scheduler::task_enum_tag<scheduler::flat_task_kind::bootstrap_resolving>,
        job.get(),
        prepared,
        parameters,
        *database_,
        tx_,
        request_ctx,
        store,
        exec
    });
    ts.wait_for_progress(*job);

    result = std::make_unique<impl::result_set>(
        std::move(store)
    );
    return request_ctx->status_code();
}

}
