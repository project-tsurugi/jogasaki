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
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/executor/sequence/sequence.h>

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;

// check and wait if async exec is on-going
void transaction::check_async_execution() {
    async_execution_latch_.wait();
}

status transaction::commit() {
    check_async_execution();
    return tx_->object()->commit();
}

status transaction::abort() {
    check_async_execution();
    return tx_->object()->abort();
}

status transaction::execute(api::executable_statement& statement) {
    std::unique_ptr<api::result_set> result{};
    return execute(statement, result);
}

status transaction::execute(
    api::executable_statement& statement,
    std::unique_ptr<api::result_set>& result
) {
    check_async_execution();
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
    request_ctx->scheduler(database_->scheduler());
    request_ctx->stmt_scheduler(
        std::make_shared<scheduler::statement_scheduler>(
            database_->configuration(),
            *database_->task_scheduler()
        )
    );
    if (e->is_execute()) {
        auto* stmt = unsafe_downcast<executor::common::execute>(e->operators().get());
        auto& g = stmt->operators();
        std::size_t cpu = sched_getcpu();
        request_ctx->job(
            std::make_shared<scheduler::job_context>(
                cpu
            )
        );

        auto& ts = *request_ctx->scheduler();
        ts.schedule_task(scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::bootstrap>,
            request_ctx.get(),
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
    scheduler::statement_scheduler sched{ database_->configuration(), *database_->task_scheduler()};
    sched.schedule(*stmt, *request_ctx);
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
    tx_(std::make_shared<transaction_context>(
        std::shared_ptr{database_->kvs_db()->create_transaction(readonly)}
    ))
{}

bool transaction::execute_async(maybe_shared_ptr<api::executable_statement> const& statement, transaction::callback on_completion) {
    return execute_async_common(statement, nullptr, std::move(on_completion));
}

bool transaction::execute_async(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<data_channel> const& channel,
    callback on_completion
) {
    return execute_async_common(statement, channel, std::move(on_completion));
}

bool transaction::execute_async_common(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    callback on_completion  //NOLINT(performance-unnecessary-value-param)
) {
    check_async_execution();
    auto& s = unsafe_downcast<impl::executable_statement&>(*statement);
    auto& e = s.body();
    auto& c = database_->configuration();
    auto rctx = std::make_shared<request_context>(
        c,
        s.resource(),
        database_->kvs_db(),
        tx_,
        database_->sequence_manager(),
        nullptr,
        channel
    );
    rctx->scheduler(database_->scheduler());
    rctx->stmt_scheduler(
        std::make_shared<scheduler::statement_scheduler>(
            database_->configuration(),
            *database_->task_scheduler()
        )
    );
    if (e->is_execute()) {
        auto* stmt = unsafe_downcast<executor::common::execute>(e->operators().get());
        auto& g = stmt->operators();
        std::size_t cpu = sched_getcpu();
        rctx->job(
            std::make_shared<scheduler::job_context>(
                cpu
            )
        );
        rctx->job()->callback([statement, on_completion, channel, rctx, this](){  // callback is copy-based
            // let lambda own the statement/channel so that they live longer by the end of callback
            (void)statement;
            (void)channel;
            on_completion(rctx->status_code(), rctx->status_message());
            async_execution_latch_.release();
        });

        async_execution_latch_.reset(); // close latch until async exec completes
        auto& ts = *rctx->scheduler();
        ts.schedule_task(scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::bootstrap>,
            rctx.get(),
            g
        });
        ts.wait_for_progress(*rctx->job());
        return true;
    }
    if(c->tasked_write()) {
        auto* stmt = unsafe_downcast<executor::common::write>(e->operators().get());
        std::size_t cpu = sched_getcpu();
        rctx->job(
            std::make_shared<scheduler::job_context>(
                cpu
            )
        );
        rctx->job()->callback([statement, on_completion, rctx, this](){  // callback is copy-based
            // let lambda own the statement/channel so that they live longer by the end of callback
            (void)statement;
            on_completion(rctx->status_code(), rctx->status_message());
            async_execution_latch_.release();
        });

        async_execution_latch_.reset(); // close latch until async exec completes
        auto& ts = *rctx->scheduler();
        ts.schedule_task(scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::write>,
            rctx.get(),
            stmt,
        });
        ts.wait_for_progress(*rctx->job());
        return true;
    }
    auto* stmt = unsafe_downcast<executor::common::write>(e->operators().get());
    scheduler::statement_scheduler sched{ database_->configuration(), *database_->task_scheduler()};
    sched.schedule(*stmt, *rctx);
    on_completion(rctx->status_code(), rctx->status_message());
    return true;
}

}
