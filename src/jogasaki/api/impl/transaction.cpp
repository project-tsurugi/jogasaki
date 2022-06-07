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

#include <jogasaki/constants.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/api/impl/result_store_channel.h>
#include <jogasaki/executor/io/record_channel_adapter.h>
#include <jogasaki/executor/io/dump_channel.h>

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;

status transaction::commit() {
    return tx_->object()->commit();
}

status transaction::abort() {
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
    auto store = std::make_unique<data::result_store>();
    auto ch = std::make_shared<result_store_channel>(maybe_shared_ptr{store.get()});
    status ret{};
    std::string msg{};
    execute_common(maybe_shared_ptr{std::addressof(statement)}, ch, [&](status st, std::string_view m){
        ret = st;
        msg = m;
    }, true);
    auto& s = unsafe_downcast<impl::executable_statement&>(statement);
    if (s.body()->is_execute()) {
        result = std::make_unique<impl::result_set>(std::move(store));
    }
    return ret;
}

impl::database& transaction::database() {
    return *database_;
}

std::vector<std::string> add_secondary_indices(std::vector<std::string> const& write_preserves, impl::database& database) {
    std::vector<std::string> ret{};
    ret.reserve(write_preserves.size()*approx_index_count_per_table);
    for(auto&& wp : write_preserves) {
        auto t = database.tables()->find_table(wp);
        if(! t) continue;
        database.tables()->each_index([&](std::string_view , std::shared_ptr<yugawara::storage::index const> const& entry) {
            if(entry->table() == *t) {
                ret.emplace_back(entry->simple_name());
            }
        });
    }
    return ret;
}

transaction::transaction(
    impl::database& database,
    bool readonly,
    bool is_long,
    std::vector<std::string> const& write_preserves
) :
    database_(std::addressof(database)),
    tx_(std::make_shared<transaction_context>(
        std::shared_ptr{database_->kvs_db()->create_transaction(readonly, is_long, add_secondary_indices(write_preserves, database))}
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
    return execute_common(
        statement,
        std::make_shared<executor::io::record_channel_adapter>(channel),
        std::move(on_completion),
        false
    );
}

bool transaction::execute_dump(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    std::string_view directory,
    callback on_completion,
    std::size_t max_records_per_file
) {
    executor::io::dump_cfg cfg{};
    cfg.max_records_per_file_ = max_records_per_file;
    return execute_common(
        statement,
        std::make_shared<executor::io::dump_channel>(
            std::make_shared<executor::io::record_channel_adapter>(channel),
            directory,
            cfg
        ),
        std::move(on_completion),
        false
    );
}

bool transaction::execute_common(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync
) {
    auto& s = unsafe_downcast<impl::executable_statement&>(*statement);
    auto& e = s.body();
    auto& c = database_->configuration();
    auto rctx = std::make_shared<request_context>(
        c,
        s.resource(),
        database_->kvs_db(),
        tx_,
        database_->sequence_manager(),
        channel
    );
    rctx->scheduler(database_->scheduler());
    rctx->stmt_scheduler(
        std::make_shared<scheduler::statement_scheduler>(
            database_->configuration(),
            *database_->task_scheduler()
        )
    );
    rctx->storage_provider(database_->tables());
    if (e->is_execute()) {
        auto* stmt = unsafe_downcast<executor::common::execute>(e->operators().get());
        auto& g = stmt->operators();
        std::size_t cpu = sched_getcpu();

        auto job = std::make_shared<scheduler::job_context>(cpu);
        rctx->job(maybe_shared_ptr{job.get()});

        job->callback([statement, on_completion, channel, rctx](){  // callback is copy-based
            // let lambda own the statement/channel so that they live longer by the end of callback
            (void)statement;
            (void)channel;
            on_completion(rctx->status_code(), rctx->status_message());
        });

        auto& ts = *rctx->scheduler();
        ts.register_job(job);
        ts.schedule_task(scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::bootstrap>,
            rctx.get(),
            g
        });
        if(ts.kind() == scheduler::task_scheduler_kind::serial || sync) {
            ts.wait_for_progress(*job);
        }
        return true;
    }
    if(!e->is_ddl() && c->tasked_write()) {
        // write on tasked mode
        auto* stmt = unsafe_downcast<executor::common::write>(e->operators().get());
        std::size_t cpu = sched_getcpu();
        auto job = std::make_shared<scheduler::job_context>(cpu);
        rctx->job(maybe_shared_ptr{job.get()});
        job->callback([statement, on_completion, rctx](){  // callback is copy-based
            // let lambda own the statement/channel so that they live longer by the end of callback
            (void)statement;
            on_completion(rctx->status_code(), rctx->status_message());
        });

        auto& ts = *rctx->scheduler();
        ts.register_job(job);
        ts.schedule_task(scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::write>,
            rctx.get(),
            stmt,
        });
        if(ts.kind() == scheduler::task_scheduler_kind::serial || sync) {
            ts.wait_for_progress(*job);
        }
        return true;
    }
    // write on non-tasked mode or DDL
    scheduler::statement_scheduler sched{ database_->configuration(), *database_->task_scheduler()};
    sched.schedule(*e->operators(), *rctx);
    on_completion(rctx->status_code(), rctx->status_message());
    return true;
}

}
