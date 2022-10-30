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
#include <takatori/util/string_builder.h>

#include <jogasaki/constants.h>
#include <jogasaki/logging.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/api/impl/result_store_channel.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/executor/io/record_channel_adapter.h>
#include <jogasaki/executor/io/dump_channel.h>
#include <jogasaki/executor/io/null_record_channel.h>
#include <jogasaki/utils/backoff_waiter.h>

#include "request_context_factory.h"

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;
using takatori::util::string_builder;

status transaction::commit() {
    auto res = commit_internal();
    if(res == status::err_waiting_for_other_transaction) {
        utils::backoff_waiter waiter{};
        while(true) {
            auto st = tx_->object()->check_state().state_kind();
            VLOG(log_debug) << "checking for waiting transaction state:" << st;
            if (st != ::sharksfin::TransactionState::StateKind::WAITING_CC_COMMIT) {
                if(auto res2 = commit_internal(); res2 == status::err_waiting_for_other_transaction) {
                    // must not happen
                    return res2;
                }
                res = status::ok;
                break;
            }
            waiter();
        }
    }
    return res;
}

status transaction::commit_internal() {
    return tx_->object()->commit();
}

status transaction::abort() {
    return tx_->object()->abort();
}

status transaction::execute(
    api::executable_statement& statement,
    std::unique_ptr<api::result_set>& result
) {
    auto store = std::make_unique<data::result_store>();
    auto ch = std::make_shared<result_store_channel>(maybe_shared_ptr{store.get()});
    status ret{};
    std::string msg{};
    execute_internal(maybe_shared_ptr{std::addressof(statement)}, ch, [&](status st, std::string_view m){
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

transaction::transaction(
    impl::database& database,
    kvs::transaction_option const& options
) :
    database_(std::addressof(database)),
    tx_(wrap(database_->kvs_db()->create_transaction(options)))
{}

status transaction::execute(
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<api::result_set>& result
) {
    auto store = std::make_unique<data::result_store>();
    auto ch = std::make_shared<result_store_channel>(maybe_shared_ptr{store.get()});
    status ret{};
    std::string msg{};
    execute_async(prepared, std::move(parameters), ch, [&](status st, std::string_view m){
        ret = st;
        msg = m;
    }, true);
    result = std::make_unique<impl::result_set>(std::move(store));
    return ret;
}

bool transaction::execute_async(
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion,
    bool sync
) {
    auto request_ctx = create_request_context(
        channel,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool())
    );
    auto& ts = *database_->task_scheduler();
    auto jobid = request_ctx->job()->id();
    ts.schedule_task(scheduler::flat_task{
        scheduler::task_enum_tag<scheduler::flat_task_kind::resolve>,
        request_ctx,
        std::make_shared<scheduler::statement_context>(
            prepared,
            std::move(parameters),
            database_,
            this,
            std::move(on_completion)
        )
    });
    if(sync) {
        ts.wait_for_progress(jobid);
    }
    return true;

}

bool transaction::execute_async(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    callback on_completion  //NOLINT(performance-unnecessary-value-param)
) {
    return execute_internal(
        statement,
        channel ?
            maybe_shared_ptr<executor::io::record_channel>{std::make_shared<executor::io::record_channel_adapter>(channel)} :
            maybe_shared_ptr<executor::io::record_channel>{std::make_shared<executor::io::null_record_channel>()},
        std::move(on_completion),
        false
    );
}

bool transaction::execute_dump(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    std::string_view directory,
    callback on_completion,
    std::size_t max_records_per_file,
    bool keep_files_on_error
) {
    executor::io::dump_cfg cfg{};
    cfg.max_records_per_file_ = max_records_per_file;
    cfg.keep_files_on_error_ = keep_files_on_error;
    auto dump_ch = std::make_shared<executor::io::dump_channel>(
        std::make_shared<executor::io::record_channel_adapter>(channel),
        directory,
        cfg
    );
    return execute_internal(
        statement,
        dump_ch,
        [on_completion=std::move(on_completion), dump_ch, cfg](status st, std::string_view msg) {
            if(st != status::ok) {
                if (! cfg.keep_files_on_error_) {
                    dump_ch->clean_output_files();
                }
            }
            on_completion(st, msg);
        },
        false
    );
}

std::shared_ptr<request_context> transaction::create_request_context(
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    std::shared_ptr<memory::lifo_paged_memory_resource> resource
) {
    return impl::create_request_context(
        database_,
        tx_,
        channel,
        std::move(resource)
    );
}

bool transaction::execute_internal(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync
) {
    BOOST_ASSERT(channel);  //NOLINT
    auto& s = unsafe_downcast<impl::executable_statement&>(*statement);
    return execute_async_on_context(
        create_request_context(channel, s.resource()),
        statement,
        std::move(on_completion),
        sync
    );
}

bool validate_statement(
    plan::executable_statement const& exec,
    maybe_shared_ptr<executor::io::record_channel> const& ch,
    transaction::callback on_completion //NOLINT(performance-unnecessary-value-param)
) {
    if(!exec.mirrors()->external_writer_meta() &&
        dynamic_cast<executor::io::record_channel_adapter*>(ch.get())) {
        // result_store_channel is for testing and error handling is not needed
        // null_record_channel is to discard the results and is correct usage
        auto msg = "statement has no result records, but called with API expecting result records";
        VLOG(log_error) << msg;
        on_completion(status::err_illegal_operation, msg);
        return false;
    }
    return true;
}

bool transaction::execute_async_on_context(
    std::shared_ptr<request_context> rctx,  //NOLINT
    maybe_shared_ptr<api::executable_statement> const& statement,
    callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync
) {
    auto& s = unsafe_downcast<impl::executable_statement&>(*statement);
    if(! validate_statement(*s.body(), rctx->record_channel(), on_completion)) {
        return false;
    }
    auto& e = s.body();
    auto job = rctx->job();
    auto& ts = *rctx->scheduler();
    if (e->is_execute()) {
        auto* stmt = unsafe_downcast<executor::common::execute>(e->operators().get());
        auto& g = stmt->operators();
        job->callback([statement, on_completion, rctx](){  // callback is copy-based
            // let lambda own the statement so that they live longer by the end of callback
            (void)statement;
            on_completion(rctx->status_code(), rctx->status_message());
        });

        auto jobid = job->id();
        ts.schedule_task(scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::bootstrap>,
            rctx.get(),
            g
        });
        if(sync) {
            ts.wait_for_progress(jobid);
        }
        return true;
    }
    if(!e->is_ddl() && rctx->configuration()->tasked_write()) {
        // write on tasked mode
        auto* stmt = unsafe_downcast<executor::common::write>(e->operators().get());
        job->callback([statement, on_completion, rctx](){  // callback is copy-based
            // let lambda own the statement so that they live longer by the end of callback
            (void)statement;
            on_completion(rctx->status_code(), rctx->status_message());
        });

        auto jobid = job->id();
        ts.schedule_task(scheduler::flat_task{
            scheduler::task_enum_tag<scheduler::flat_task_kind::write>,
            rctx.get(),
            stmt,
        });
        if(sync) {
            ts.wait_for_progress(jobid);
        }
        return true;
    }
    // write on non-tasked mode or DDL
    scheduler::statement_scheduler sched{ database_->configuration(), *database_->task_scheduler()};
    sched.schedule(*e->operators(), *rctx);
    on_completion(rctx->status_code(), rctx->status_message());
    ts.unregister_job(job->id());
    return true;
}

bool transaction::execute_load(
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::vector<std::string> files,
    transaction::callback on_completion
) {
    auto rctx = create_request_context(
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool())
    );
    auto ldr = std::make_shared<executor::file::loader>(
        std::move(files),
        prepared,
        std::move(parameters),
        this
    );
    rctx->job()->callback([on_completion=std::move(on_completion), rctx, ldr](){  // callback is copy-based
        (void)ldr; // to keep ownership
        on_completion(rctx->status_code(), rctx->status_message());
    });
    auto& ts = *rctx->scheduler();
    ts.schedule_task(scheduler::flat_task{
        scheduler::task_enum_tag<scheduler::flat_task_kind::load>,
        rctx.get(),
        std::move(ldr)
    });
    return true;
}

bool transaction::commit_async(transaction::callback on_completion) {
    auto rctx = create_request_context(
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool())
    );
    auto t = scheduler::create_custom_task(rctx.get(),
        [this, rctx]() {
            auto res = commit_internal();
            if(res == status::err_waiting_for_other_transaction) {
                return model::task_result::yield;
            }
            rctx->status_code(res);
            if(res != status::ok) {
                rctx->status_message(
                    string_builder{} << "commit failed with error:" << res << string_builder::to_string
                );
            }
            return model::task_result::complete;
        }, true);
    rctx->job()->callback([on_completion=std::move(on_completion), rctx](){  // callback is copy-based
        on_completion(rctx->status_code(), rctx->status_message());
    });
    auto& ts = *rctx->scheduler();
    ts.schedule_task(std::move(t));
    return true;
}

bool transaction::transaction::is_ready() {
    auto st = tx_->object()->check_state().state_kind();
    return st != ::sharksfin::TransactionState::StateKind::WAITING_START;
}

}
