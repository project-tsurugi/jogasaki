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
#include "executor.h"

#include <takatori/util/downcast.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/accessor/record_printer.h>
#include <jogasaki/api/executable_statement.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/request_context_factory.h>
#include <jogasaki/api/impl/result_set.h>
#include <jogasaki/api/impl/result_store_channel.h>
#include <jogasaki/constants.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/common/execute.h>
#include <jogasaki/executor/common/write.h>
#include <jogasaki/executor/io/dump_channel.h>
#include <jogasaki/executor/io/null_record_channel.h>
#include <jogasaki/executor/io/record_channel_adapter.h>
#include <jogasaki/executor/sequence/sequence.h>
#include <jogasaki/external_log/event_logging.h>
#include <jogasaki/external_log/events.h>
#include <jogasaki/index/index_accessor.h>
#include <jogasaki/index/utils.h>
#include <jogasaki/kvs/readable_stream.h>
#include <jogasaki/logging.h>
#include <jogasaki/logging_helper.h>
#include <jogasaki/plan/compiler.h>
#include <jogasaki/request_logging.h>
#include <jogasaki/scheduler/flat_task.h>
#include <jogasaki/scheduler/job_context.h>
#include <jogasaki/scheduler/task_factory.h>
#include <jogasaki/scheduler/task_scheduler.h>
#include <jogasaki/utils/abort_error.h>
#include <jogasaki/utils/external_log_utils.h>
#include <jogasaki/utils/hex.h>
#include "jogasaki/index/field_factory.h"

namespace jogasaki::executor {

using takatori::util::unsafe_downcast;
using takatori::util::string_builder;

constexpr static std::string_view log_location_prefix = "/:jogasaki:executor ";

namespace details {

bool execute_internal(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    error_info_stats_callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync,
    request_info const& req_info
) {
    BOOST_ASSERT(channel);  //NOLINT
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::execute_statement);
    req->status(scheduler::request_detail_status::accepted);
    req->transaction_id(tx->transaction_id());
    auto const& stmt = static_cast<api::impl::executable_statement*>(statement.get())->body(); //NOLINT
    req->statement_text(stmt->sql_text_shared());
    log_request(*req);

    auto& s = unsafe_downcast<api::impl::executable_statement&>(*statement);
    auto rctx = create_request_context(database, std::move(tx), channel, s.resource(), std::move(req));
    rctx->lightweight(
        stmt->mirrors()->work_level().value() <=
        static_cast<std::int32_t>(rctx->configuration()->lightweight_job_level())
    );
    return execute_async_on_context(
        database,
        std::move(rctx),
        statement,
        std::move(on_completion),
        sync,
        req_info
    );
}

status init(
    api::impl::database& database,
    std::shared_ptr<kvs::transaction_option const> options,
    std::shared_ptr<transaction_context>& out
) {
    std::unique_ptr<kvs::transaction> kvs_tx{};
    if(auto res = kvs::transaction::create_transaction(*database.kvs_db(), kvs_tx, *options); res != status::ok) {
        return res;
    }
    out = wrap(std::move(kvs_tx), std::move(options));

    return status::ok;
}

}
status commit(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    api::commit_option option  //NOLINT(performance-unnecessary-value-param)
) {
    std::atomic<status> ret{};
    auto jobid = commit_async(
            database,
            std::move(tx),
            [&](
                status st,
                std::shared_ptr<error::error_info> info  //NOLINT(performance-unnecessary-value-param)
            ){
                ret.store(st);
                if(st != status::ok) {
                    VLOG(log_error) << log_location_prefix << (info ? info->message() : "");
                }
            },
            option,
            request_info{}
    );
    database.task_scheduler()->wait_for_progress(jobid);
    return ret;
}

status abort_transaction(
    std::shared_ptr<transaction_context> tx,  //NOLINT(performance-unnecessary-value-param)
    request_info const& req_info
) {
    std::string txid{tx->transaction_id()};
    auto ret = tx->object()->abort();
    VLOG(log_debug_timing_event) << "/:jogasaki:timing:transaction:finished "
        << txid
        << " status:"
        << (ret == status::ok ? "aborted" : "error"); // though we do not expect abort fails
    if(ret == status::ok) {
        // TODO abort is almost always successful. distinguish "real abort"
        auto tx_type = utils::tx_type_from(*tx);
        external_log::tx_end(req_info, "", txid, tx_type, external_log::result_value::fail);
    }
    return ret;
}

status execute(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    api::executable_statement& statement,
    std::unique_ptr<api::result_set>& result,
    std::shared_ptr<error::error_info>& error,
    std::shared_ptr<request_statistics>& stats,
    request_info const& req_info
) {
    auto store = std::make_unique<data::result_store>();
    auto ch = std::make_shared<api::impl::result_store_channel>(maybe_shared_ptr{store.get()});
    status ret{};
    details::execute_internal(
            database,
            std::move(tx),
            maybe_shared_ptr{std::addressof(statement)},
            ch,
            [&](
                status st,
                std::shared_ptr<error::error_info> info,  //NOLINT(performance-unnecessary-value-param)
                std::shared_ptr<request_statistics> statistics  //NOLINT(performance-unnecessary-value-param)
            ) {
                ret = st;
                error = std::move(info);
                stats = std::move(statistics);
            },
            true,
            req_info
    );
    auto& s = unsafe_downcast<api::impl::executable_statement&>(statement);
    if (s.body()->is_execute()) {
        result = std::make_unique<api::impl::result_set>(std::move(store));
    }
    return ret;
}

status execute(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    std::unique_ptr<api::result_set>& result,
    std::shared_ptr<error::error_info>& error,
    std::shared_ptr<request_statistics>& stats,
    request_info const& req_info
) {
    auto store = std::make_unique<data::result_store>();
    auto ch = std::make_shared<api::impl::result_store_channel>(maybe_shared_ptr{store.get()});
    status ret{};
    execute_async(database, std::move(tx), prepared, std::move(parameters), ch,
        [&](
            status st,
            std::shared_ptr<error::error_info> info,   //NOLINT(performance-unnecessary-value-param)
            std::shared_ptr<request_statistics> statistics //NOLINT(performance-unnecessary-value-param)
        ){
            ret = st;
            error = std::move(info);
            stats = std::move(statistics);
    }, true, req_info);
    result = std::make_unique<api::impl::result_set>(std::move(store));
    return ret;
}

bool execute_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,  //NOLINT(performance-unnecessary-value-param)
    api::statement_handle prepared,
    std::shared_ptr<api::parameter_set> parameters,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    error_info_stats_callback on_completion,
    bool sync,
    request_info const& req_info
) {
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::execute_statement);
    req->status(scheduler::request_detail_status::accepted);
    auto const& stmt = reinterpret_cast<api::impl::prepared_statement*>(prepared.get())->body();  //NOLINT
    req->statement_text(stmt->sql_text_shared());
    log_request(*req);

    auto request_ctx = create_request_context(
        database,
        tx,
        channel,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        req
    );
    request_ctx->req_info(req_info);
    request_ctx->lightweight(
        stmt->mirrors()->work_level().value() <=
        static_cast<std::int32_t>(request_ctx->configuration()->lightweight_job_level())
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
    std::shared_ptr<transaction_context> tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    error_info_stats_callback on_completion,  //NOLINT(performance-unnecessary-value-param)
    request_info const& req_info
) {
    return details::execute_internal(
        database,
        std::move(tx),
        statement,
        channel ? maybe_shared_ptr<executor::io::record_channel>{std::make_shared<executor::io::record_channel_adapter>(
                      channel
                  )}
                : maybe_shared_ptr<executor::io::record_channel>{std::make_shared<executor::io::null_record_channel>()},
        std::move(on_completion),
        false,
        req_info
    );
}

bool execute_dump(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<api::data_channel> const& channel,
    std::string_view directory,
    error_info_callback on_completion,
    io::dump_config const& cfg,
    request_info const& req_info
) {
    auto dump_ch = std::make_shared<executor::io::dump_channel>(
        std::make_shared<executor::io::record_channel_adapter>(channel),
        directory,
        cfg
    );
    return details::execute_internal(
        database,
        std::move(tx),
        statement,
        dump_ch,
        [on_completion=std::move(on_completion), dump_ch, cfg](
            status st,
            std::shared_ptr<error::error_info> info,
            std::shared_ptr<request_statistics> stats  //NOLINT(performance-unnecessary-value-param)
        ) {
            (void) stats; // no stats for dump yet
            if(st != status::ok) {
                if (! cfg.keep_files_on_error_) {
                    dump_ch->clean_output_files();
                }
            }
            on_completion(st, std::move(info));
        },
        false,
        req_info
    );
}


bool validate_statement(
    plan::executable_statement const& exec,
    maybe_shared_ptr<executor::io::record_channel> const& ch,
    error_info_stats_callback on_completion //NOLINT(performance-unnecessary-value-param)
) {
    if(!exec.mirrors()->external_writer_meta() &&
        dynamic_cast<executor::io::record_channel_adapter*>(ch.get())) {
        // result_store_channel is for testing and error handling is not needed
        // null_record_channel is to discard the results and is correct usage
        auto msg = "statement has no result records, but called with API expecting result records";
        VLOG_LP(log_error) << msg;
        auto res = status::err_illegal_operation;
        on_completion(res, create_error_info(error_code::inconsistent_statement_exception, msg, res), nullptr);
        return false;
    }
    return true;
}

void external_log_stmt_start(
    request_context& rctx,
    request_info const& req_info,
    maybe_shared_ptr<api::executable_statement> const& statement
) {
    auto tx_id = rctx.transaction()->transaction_id();
    auto tx_type = utils::tx_type_from(*rctx.transaction());
    auto job = rctx.job();
    auto jobid = job->id();
    auto jobidstr = string_builder{} << utils::hex(jobid) << string_builder::to_string;
    auto stmt = static_cast<api::impl::executable_statement*>(statement.get())->body()->sql_text();
    external_log::stmt_start(req_info, "", tx_id, tx_type, jobidstr, stmt, "");  // TODO stringify parameters
}

void external_log_stmt_end(
    request_context& rctx,
    request_info const& req_info,
    maybe_shared_ptr<api::executable_statement> const& statement
) {
    auto tx_id = rctx.transaction()->transaction_id();
    auto tx_type = utils::tx_type_from(*rctx.transaction());
    auto job = rctx.job();
    auto jobid = job->id();
    auto jobidstr = string_builder{} << utils::hex(jobid) << string_builder::to_string;
    auto stmt = static_cast<api::impl::executable_statement*>(statement.get())->body()->sql_text();
    auto result = utils::result_from(rctx.status_code());
    auto state_code =
        rctx.error_info() ? "SQL-" + std::to_string(static_cast<std::int64_t>(rctx.error_info()->code())) : "";
    std::int64_t inserted{};
    std::int64_t updated{};
    std::int64_t deleted{};
    std::int64_t merged{};
    if(rctx.stats()) {
        if(auto cnt = rctx.stats()->counter(counter_kind::inserted).count(); cnt.has_value()) {
            inserted = cnt.value();
        }
        if(auto cnt = rctx.stats()->counter(counter_kind::updated).count(); cnt.has_value()) {
            updated = cnt.value();
        }
        if(auto cnt = rctx.stats()->counter(counter_kind::deleted).count(); cnt.has_value()) {
            deleted = cnt.value();
        }
        if(auto cnt = rctx.stats()->counter(counter_kind::merged).count(); cnt.has_value()) {
            merged = cnt.value();
        }
    }
    external_log::stmt_end(
        req_info,
        "",
        tx_id,
        tx_type,
        jobidstr,
        stmt,
        "",  // TODO stringify parameters
        result,
        state_code,
        0, //TODO fetched
        inserted,
        updated,
        deleted,
        merged
    );
}

void external_log_stmt_explain(
    api::impl::database& database,
    request_context& rctx,
    request_info const& req_info,
    maybe_shared_ptr<api::executable_statement> const& statement
) {
    auto cfg = global::config_pool();
    if(! cfg || ! cfg->external_log_explain()) {
        return;
    }
    auto tx_id = rctx.transaction()->transaction_id();
    auto tx_type = utils::tx_type_from(*rctx.transaction());
    auto job = rctx.job();
    auto jobid = job->id();
    auto jobidstr = string_builder{} << utils::hex(jobid) << string_builder::to_string;
    std::stringstream ss{};
    database.explain(*statement, ss);
    external_log::stmt_explain(
        req_info,
        tx_id,
        tx_type,
        jobidstr,
        ss.str()
    );
}

bool execute_async_on_context(
    api::impl::database& database,
    std::shared_ptr<request_context> rctx,  //NOLINT
    maybe_shared_ptr<api::executable_statement> const& statement,
    error_info_stats_callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync,
    request_info const& req_info
) {
    auto& s = unsafe_downcast<api::impl::executable_statement&>(*statement);
    if(! validate_statement(*s.body(), rctx->record_channel(), on_completion)) {
        return false;
    }
    rctx->enable_stats();
    auto& e = s.body();
    auto job = rctx->job();
    auto& ts = *rctx->scheduler();

    external_log_stmt_start(*rctx, req_info, statement);
    external_log_stmt_explain(database, *rctx, req_info, statement);

    if (e->is_execute()) {
        auto* stmt = unsafe_downcast<executor::common::execute>(e->operators().get());
        auto& g = stmt->operators();
        job->callback([statement, on_completion, rctx, job, req_info](){  // callback is copy-based
            // let lambda own the statement so that they live longer by the end of callback
            (void)statement;
            external_log_stmt_end(*rctx, req_info, statement);
            on_completion(rctx->status_code(), rctx->error_info(), rctx->stats());
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
    if(!e->is_ddl() && !e->is_empty()) {
        // write on tasked mode
        auto* stmt = unsafe_downcast<executor::common::write>(e->operators().get());
        job->callback([statement, on_completion, rctx, req_info](){  // callback is copy-based
            // let lambda own the statement so that they live longer by the end of callback
            (void)statement;
            external_log_stmt_end(*rctx, req_info, statement);
            on_completion(rctx->status_code(), rctx->error_info(), rctx->stats());
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
    // DDL
    scheduler::statement_scheduler sched{ database.configuration(), *database.task_scheduler()};
    sched.schedule(*e->operators(), *rctx);
    external_log_stmt_end(*rctx, req_info, statement);
    on_completion(rctx->status_code(), rctx->error_info(), rctx->stats());
    if(auto req = job->request()) {
        req->status(scheduler::request_detail_status::finishing);
        log_request(*req, rctx->status_code() == status::ok);
    }
    ts.unregister_job(job->id());
    return true;
}

bool execute_load(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx,  //NOLINT(performance-unnecessary-value-param)
    api::statement_handle prepared,
    maybe_shared_ptr<api::parameter_set const> parameters,
    std::vector<std::string> files,
    error_info_callback on_completion,
    request_info const& req_info
) {
    (void) req_info;
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::load);
    req->status(scheduler::request_detail_status::accepted);
    req->statement_text(reinterpret_cast<api::impl::prepared_statement*>(prepared.get())->body()->sql_text_shared());  //NOLINT
    log_request(*req);

    auto rctx = create_request_context(
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
        on_completion(rctx->status_code(), rctx->error_info());
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

void process_commit_callback(
    ::sharksfin::StatusCode st,
    ::sharksfin::ErrorCode ec,
    ::sharksfin::durability_marker_type marker,
    std::size_t const& jobid,
    std::shared_ptr<request_context> const& rctx,
    std::string const& txid,
    api::impl::database& database,
    api::commit_option const& option
) {
    [[maybe_unused]] auto inprocess_requests = database.requests_inprocess();
    if(database.stop_requested()) return;
    (void) ec;
    VLOG(log_debug_timing_event) << "/:jogasaki:timing:committing_end "
        << txid
        << " job_id:"
        << utils::hex(jobid);
    auto res = kvs::resolve(st);
    if(res != status::ok) {
        auto& ts = *rctx->scheduler();
        ts.schedule_task(
            scheduler::create_custom_task(rctx.get(), [rctx, res]() {
                auto msg = utils::create_abort_message(*rctx);
                auto code = res == status::err_inactive_transaction ?
                    error_code::inactive_transaction_exception :
                    error_code::cc_exception;
                set_error(
                    *rctx,
                    code,
                    msg,
                    res
                );
                scheduler::submit_teardown(*rctx);
                return model::task_result::complete;
            }, false)
        );
        return;
    }
    rctx->transaction()->durability_marker(marker);

    // if auto dispose
    if (option.auto_dispose_on_success()) {
        api::transaction_handle handle{rctx->transaction().get(), std::addressof(database)};
        if (auto rc = database.destroy_transaction(handle); rc != jogasaki::status::ok) {
            VLOG(log_error) << log_location_prefix << "unexpected error destroying transaction: " << rc;
        }
    }
    auto cr = rctx->transaction()->commit_response();
    if(cr == commit_response_kind::accepted || cr == commit_response_kind::available) {
        scheduler::submit_teardown(*rctx);
        return;
    }
    // commit_response = stored, propagated, or undefined
    // current marker should have been set at least once on callback registration
    if(marker <= database.durable_manager()->current_marker()) {
        scheduler::submit_teardown(*rctx);
        return;
    }
    database.durable_manager()->add_to_waitlist(rctx);
}

scheduler::job_context::job_id_type commit_async(
    api::impl::database& database,
    std::shared_ptr<transaction_context> tx, //NOLINT(performance-unnecessary-value-param)
    error_info_callback on_completion,
    api::commit_option option,
    request_info const& req_info
) {
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::commit);
    req->status(scheduler::request_detail_status::accepted);
    req->transaction_id(tx->transaction_id());
    log_request(*req);

    auto rctx = create_request_context(
        database,
        tx,
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        req
    );
    auto jobid = rctx->job()->id();
    std::string txid{tx->transaction_id()};

    auto cr = option.commit_response() != commit_response_kind::undefined ?
        option.commit_response() :
        database.config()->default_commit_response();
    tx->commit_response(cr);

    auto t = scheduler::create_custom_task(rctx.get(), [&database, rctx, jobid, txid, option]() {
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:committing "
            << txid
            << " job_id:"
            << utils::hex(jobid);
        rctx->transaction()->profile()->set_commit_requested();
        [[maybe_unused]] auto b = rctx->transaction()->commit(
            [jobid, rctx, txid, &database, option](
                ::sharksfin::StatusCode st,
                ::sharksfin::ErrorCode ec,
                ::sharksfin::durability_marker_type marker
            ){
                rctx->transaction()->profile()->set_precommit_cb_invoked();
                process_commit_callback(st, ec, marker, jobid, rctx, txid, database, option);
            });
        return model::task_result::complete;
    }, true);
    rctx->job()->callback([on_completion=std::move(on_completion), rctx, jobid, txid, req_info](){  // callback is copy-based
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:committed "
            << txid
            << " job_id:"
            << utils::hex(jobid);
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:transaction:finished "
            << txid
            << " status:"
            << (rctx->status_code() == status::ok ? "committed" : "aborted");
        rctx->transaction()->profile()->set_commit_job_completed();
        auto tx_type = utils::tx_type_from(*rctx->transaction());
        auto result = utils::result_from(rctx->status_code());
        external_log::tx_end(req_info, "", txid, tx_type, result);
        on_completion(rctx->status_code(), rctx->error_info());
    });
    std::weak_ptr wrctx{rctx};
    rctx->job()->completion_readiness([wrctx=std::move(wrctx)]() {
        // The job completion needs to wait for the commit callback released by cc engine.
        // Because otherwise callback destruction (and that of req. and tx contexts) in cc engine results in
        // api call (such as shirakami::leave) made from inside cc engine.
        // This kind of reentrancy is not assured by the cc engine api, so the job completion should be delayed
        // so that teardown becomes the last to release those context objects.
        return wrctx.use_count() <= 1;
    });
    auto& ts = *rctx->scheduler();
    req->status(scheduler::request_detail_status::submitted);
    log_request(*req);
    ts.schedule_task(std::move(t));
    return jobid;
}

status create_transaction(
    api::impl::database& db,
    std::shared_ptr<transaction_context>& out,
    std::shared_ptr<kvs::transaction_option const> options
) {
    std::shared_ptr<transaction_context> ret{};
    if(auto res = details::init(db, std::move(options), ret); res != status::ok) {
        return res;
    }
    out = std::move(ret);
    out->profile()->enabled(db.config()->profile_commits());
    return status::ok;
}

}  // namespace jogasaki::executor
