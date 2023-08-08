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
#include "executor.h"

#include <takatori/util/downcast.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/constants.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/api/impl/result_store_channel.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/executor/io/record_channel_adapter.h>
#include <jogasaki/executor/io/dump_channel.h>
#include <jogasaki/executor/io/null_record_channel.h>
#include <jogasaki/utils/backoff_timer.h>
#include <jogasaki/utils/abort_error.h>
#include <jogasaki/utils/hex.h>
#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/index/index_accessor.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/request_logging.h>

#include "jogasaki/index/field_factory.h"

namespace jogasaki::executor {

using takatori::util::unsafe_downcast;
using takatori::util::string_builder;

constexpr static std::string_view log_location_prefix = "/:jogasaki:api:impl:transaction ";

status commit(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx
) {
    status ret{};
    auto jobid = commit_async(
            database,
            tx,
            [&](status st, std::string_view m){
                ret = st;
                if(st != status::ok) {
                    VLOG(log_error) << log_location_prefix << m;
                }
            }
    );
    database.task_scheduler()->wait_for_progress(jobid);
    return ret;
}

status commit_internal(
    std::shared_ptr<transaction_context> const& tx
) {
    return tx->object()->commit();
}

status abort(
    std::shared_ptr<transaction_context> const& tx
) {
    std::string txid{tx->object()->transaction_id()};
    auto ret = tx->object()->abort();
    VLOG(log_debug_timing_event) << "/:jogasaki:timing:transaction:finished "
        << txid
        << " status:"
        << (ret == status::ok ? "aborted" : "error"); // though we do not expect abort fails
    return ret;
}

status execute(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    api::executable_statement& statement,
    std::unique_ptr<api::result_set>& result
) {
    auto store = std::make_unique<data::result_store>();
    auto ch = std::make_shared<api::impl::result_store_channel>(maybe_shared_ptr{store.get()});
    status ret{};
    std::string msg{};
    details::execute_internal(
            database,
            tx,
            maybe_shared_ptr{std::addressof(statement)},
            ch,
            [&](status st, std::string_view m) {
                ret = st;
                msg = m;
            },
            true
    );
    auto& s = unsafe_downcast<api::impl::executable_statement&>(statement);
    if (s.body()->is_execute()) {
        result = std::make_unique<api::impl::result_set>(std::move(store));
    }
    return ret;
}

status execute(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<api::result_set>& result
) {
    auto store = std::make_unique<data::result_store>();
    auto ch = std::make_shared<api::impl::result_store_channel>(maybe_shared_ptr{store.get()});
    status ret{};
    std::string msg{};
    execute_async(database, tx, prepared, std::move(parameters), ch, [&](status st, std::string_view m){
        ret = st;
        msg = m;
    }, true);
    result = std::make_unique<api::impl::result_set>(std::move(store));
    return ret;
}

bool execute_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion,
    bool sync
) {
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::execute_statement);
    req->status(scheduler::request_detail_status::accepted);
    auto const& stmt = reinterpret_cast<api::impl::prepared_statement*>(prepared.get())->body();  //NOLINT
    req->statement_text(stmt->sql_text_shared());
    log_request(*req);

    auto request_ctx = details::create_request_context(
        database,
        tx,
        channel,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        req
    );
    request_ctx->lightweight(
            stmt->mirrors()->work_level().value() <= static_cast<std::int32_t>(request_ctx->configuration()->lightweight_job_level())
    );
    auto& ts = *database.task_scheduler();
    auto jobid = request_ctx->job()->id();

    req->status(scheduler::request_detail_status::submitted);
    log_request(*req);
    ts.schedule_task(scheduler::flat_task{
        scheduler::task_enum_tag<scheduler::flat_task_kind::resolve>,
        request_ctx,
        std::make_shared<scheduler::statement_context>(
            prepared,
            std::move(parameters),
            std::addressof(database),
            tx,
            std::move(on_completion)
        )
    });
    if(sync) {
        ts.wait_for_progress(jobid);
    }
    return true;

}

bool execute_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    callback on_completion  //NOLINT(performance-unnecessary-value-param)
) {
    return details::execute_internal(
        database,
        tx,
        statement,
        channel ?
            maybe_shared_ptr<executor::io::record_channel>{std::make_shared<executor::io::record_channel_adapter>(channel)} :
            maybe_shared_ptr<executor::io::record_channel>{std::make_shared<executor::io::null_record_channel>()},
        std::move(on_completion),
        false
    );
}

bool execute_dump(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
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
    return details::execute_internal(
        database,
        tx,
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

namespace details {

std::shared_ptr<request_context> create_request_context(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    std::shared_ptr<memory::lifo_paged_memory_resource> resource,
    std::shared_ptr<scheduler::request_detail> request_detail
) {
    return api::impl::create_request_context(
        std::addressof(database),
        tx,
        channel,
        std::move(resource),
        std::move(request_detail)
    );
}

bool execute_internal(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync
) {
    BOOST_ASSERT(channel);  //NOLINT
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::execute_statement);
    req->status(scheduler::request_detail_status::accepted);
    req->transaction_id(transaction_id(tx));
    auto const& stmt = static_cast<api::impl::executable_statement*>(statement.get())->body(); //NOLINT
    req->statement_text(stmt->sql_text_shared());
    log_request(*req);

    auto& s = unsafe_downcast<api::impl::executable_statement&>(*statement);
    auto rctx = details::create_request_context(database, tx, channel, s.resource(), std::move(req));
    rctx->lightweight(
        stmt->mirrors()->work_level().value() <=
            static_cast<std::int32_t>(rctx->configuration()->lightweight_job_level())
    );
    return execute_async_on_context(
        database,
        tx,
        std::move(rctx),
        statement,
        std::move(on_completion),
        sync
    );
}

status init(
        api::impl::database& database,
        kvs::transaction_option const& options,
        std::shared_ptr<transaction_context>& out
) {
    std::unique_ptr<kvs::transaction> kvs_tx{};
    if(auto res = kvs::transaction::create_transaction(*database.kvs_db(), kvs_tx, options); res != status::ok) {
        return res;
    }
    out = wrap(std::move(kvs_tx));
    return status::ok;
}

}

bool validate_statement(
    plan::executable_statement const& exec,
    maybe_shared_ptr<executor::io::record_channel> const& ch,
    callback on_completion //NOLINT(performance-unnecessary-value-param)
) {
    if(!exec.mirrors()->external_writer_meta() &&
        dynamic_cast<executor::io::record_channel_adapter*>(ch.get())) {
        // result_store_channel is for testing and error handling is not needed
        // null_record_channel is to discard the results and is correct usage
        auto msg = "statement has no result records, but called with API expecting result records";
        VLOG_LP(log_error) << msg;
        on_completion(status::err_illegal_operation, msg);
        return false;
    }
    return true;
}

bool execute_async_on_context(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    std::shared_ptr<request_context> rctx,  //NOLINT
    maybe_shared_ptr<api::executable_statement> const& statement,
    callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync
) {
    (void) tx;
    auto& s = unsafe_downcast<api::impl::executable_statement&>(*statement);
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
        if(auto req = job->request()) {
            req->status(scheduler::request_detail_status::submitted);
            log_request(*req);
        }
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
        if(auto req = job->request()) {
            req->status(scheduler::request_detail_status::submitted);
            log_request(*req);
        }
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
    scheduler::statement_scheduler sched{ database.configuration(), *database.task_scheduler()};
    sched.schedule(*e->operators(), *rctx);
    on_completion(rctx->status_code(), rctx->status_message());
    if(auto req = job->request()) {
        req->status(scheduler::request_detail_status::finishing);
        log_request(*req, rctx->status_code() == status::ok);
    }
    ts.unregister_job(job->id());
    return true;
}

bool execute_load(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::vector<std::string> files,
    callback on_completion
) {
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::load);
    req->status(scheduler::request_detail_status::accepted);
    req->statement_text(reinterpret_cast<api::impl::prepared_statement*>(prepared.get())->body()->sql_text_shared());  //NOLINT
    log_request(*req);

    auto rctx = details::create_request_context(
        database,
        tx,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        req
    );
    auto ldr = std::make_shared<executor::file::loader>(
        std::move(files),
        prepared,
        std::move(parameters),
        tx,
        database
    );
    rctx->job()->callback([on_completion=std::move(on_completion), rctx, ldr](){  // callback is copy-based
        (void)ldr; // to keep ownership
        on_completion(rctx->status_code(), rctx->status_message());
    });
    auto& ts = *rctx->scheduler();
    req->status(scheduler::request_detail_status::submitted);
    log_request(*req);

    ts.schedule_task(scheduler::flat_task{
        scheduler::task_enum_tag<scheduler::flat_task_kind::load>,
        rctx.get(),
        std::move(ldr)
    });
    return true;
}

void submit_task_commit_wait(request_context* rctx, scheduler::task_body_type&& body) {
    // wait task does not need to be sticky because multiple commit operation for a transaction doesn't happen concurrently
    auto t = scheduler::create_custom_task(rctx, std::move(body), false, true);
    auto& ts = *rctx->scheduler();
    ts.schedule_task(std::move(t));
}

scheduler::job_context::job_id_type commit_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> const& tx,
    callback on_completion
) {
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::commit);
    req->status(scheduler::request_detail_status::accepted);
    req->transaction_id(transaction_id(tx));
    log_request(*req);

    auto rctx = details::create_request_context(
        database,
        tx,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        req
    );
    auto timer = std::make_shared<utils::backoff_timer>();
    auto jobid = rctx->job()->id();
    std::string txid{tx->object()->transaction_id()};
    auto t = scheduler::create_custom_task(rctx.get(), [&database, tx, rctx, timer=std::move(timer), jobid, txid]() {
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:committing "
            << txid
            << " job_id:"
            << utils::hex(jobid);
        auto res = commit_internal(tx);
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:committing_end "
            << txid
            << " job_id:"
            << utils::hex(jobid);
        if(res == status::waiting_for_other_transaction) {
            timer->reset();
            submit_task_commit_wait(rctx.get(), [rctx, timer, tx, &database]() {
                if(! (*timer)()) return model::task_result::yield;
                auto st = tx->object()->check_state().state_kind();
                switch(st) {
                    case ::sharksfin::TransactionState::StateKind::WAITING_CC_COMMIT:
                        return model::task_result::yield;
                    case ::sharksfin::TransactionState::StateKind::ABORTED: {
                        // get result and return error info
                        rctx->status_code(
                            status::err_serialization_failure,
                            utils::create_abort_message(*rctx, *tx, *database.tables()));
                        break;
                    }
                    case ::sharksfin::TransactionState::StateKind::WAITING_DURABLE:
                        break;
                    case ::sharksfin::TransactionState::StateKind::DURABLE:
                        break;
                    default: {
                        scheduler::submit_teardown(*rctx); // try to recover as much as possible
                        throw std::logic_error(string_builder{} << "wrong state:" << st << string_builder::to_string);
                    }
                }
                scheduler::submit_teardown(*rctx);
                return model::task_result::complete;
            });
            return model::task_result::complete;
        }

        auto msg = res != status::ok ? utils::create_abort_message(*rctx, *tx, *database.tables()) : "";
        rctx->status_code(res, msg);
        scheduler::submit_teardown(*rctx);
        return model::task_result::complete;
    }, true);
    rctx->job()->callback([on_completion=std::move(on_completion), rctx, jobid, txid](){  // callback is copy-based
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:committed "
            << txid
            << " job_id:"
            << utils::hex(jobid);
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:transaction:finished "
            << txid
            << " status:"
            << (rctx->status_code() == status::ok ? "committed" : "aborted");
        on_completion(rctx->status_code(), rctx->status_message());
    });
    auto& ts = *rctx->scheduler();
    req->status(scheduler::request_detail_status::submitted);
    log_request(*req);
    ts.schedule_task(std::move(t));
    return jobid;
}

bool is_ready(
    std::shared_ptr<transaction_context> const& tx
) {
    auto st = tx->object()->check_state().state_kind();
    return st != ::sharksfin::TransactionState::StateKind::WAITING_START;
}

status create_transaction(
    api::impl::database& db,
    std::shared_ptr<transaction_context>& out,
    kvs::transaction_option const& options
) {
    std::shared_ptr<transaction_context> ret{};
    if(auto res = details::init(db, options, ret); res != status::ok) {
        return res;
    }
    out = std::move(ret);
    return status::ok;
}

std::string_view transaction_id(
        std::shared_ptr<transaction_context> const& tx
) noexcept {
    return tx->object()->transaction_id();
}

}
