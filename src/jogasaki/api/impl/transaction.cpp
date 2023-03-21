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

#include "request_context_factory.h"
#include "jogasaki/index/field_factory.h"

namespace jogasaki::api::impl {

using takatori::util::unsafe_downcast;
using takatori::util::string_builder;

constexpr static std::string_view log_location_prefix = "/:jogasaki:api:impl:transaction ";

status transaction::commit() {
    status ret{};
    auto jobid = commit_async([&](status st, std::string_view m){
        ret = st;
        if(st != status::ok) {
            VLOG(log_error) << log_location_prefix << m;
        }
    });
    database_->task_scheduler()->wait_for_progress(jobid);
    return ret;
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
    impl::database& database
) :
    database_(std::addressof(database))
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
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::execute_statement);
    req->status(scheduler::request_detail_status::accepted);
    req->statement_text(reinterpret_cast<impl::prepared_statement*>(prepared.get())->body()->sql_text_shared());
    log_request(*req);

    auto request_ctx = create_request_context(
        channel,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        req
    );
    auto& ts = *database_->task_scheduler();
    auto jobid = request_ctx->job()->id();

    req->status(scheduler::request_detail_status::submitted);
    log_request(*req);
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
    std::shared_ptr<memory::lifo_paged_memory_resource> resource,
    std::shared_ptr<scheduler::request_detail> request_detail
) {
    return impl::create_request_context(
        database_,
        tx_,
        channel,
        std::move(resource),
        std::move(request_detail)
    );
}

bool transaction::execute_internal(
    maybe_shared_ptr<api::executable_statement> const& statement,
    maybe_shared_ptr<executor::io::record_channel> const& channel,
    callback on_completion, //NOLINT(performance-unnecessary-value-param)
    bool sync
) {
    BOOST_ASSERT(channel);  //NOLINT
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::execute_statement);
    req->status(scheduler::request_detail_status::accepted);
    req->transaction_id(transaction_id());
    req->statement_text(static_cast<api::impl::executable_statement*>(statement.get())->body()->sql_text_shared());
    log_request(*req);

    auto& s = unsafe_downcast<impl::executable_statement&>(*statement);
    return execute_async_on_context(
        create_request_context(channel, s.resource(), std::move(req)),
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
        VLOG(log_error)  << log_location_prefix << msg;
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
    scheduler::statement_scheduler sched{ database_->configuration(), *database_->task_scheduler()};
    if(auto req = job->request()) {
        req->status(scheduler::request_detail_status::submitted);
        log_request(*req);
    }
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
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::load);
    req->status(scheduler::request_detail_status::accepted);
    req->statement_text(reinterpret_cast<impl::prepared_statement*>(prepared.get())->body()->sql_text_shared());
    log_request(*req);

    auto rctx = create_request_context(
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        req
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
    auto t = scheduler::create_custom_task(rctx, std::move(body), true, true);
    auto& ts = *rctx->scheduler();
    ts.schedule_task(std::move(t));
}

scheduler::job_context::job_id_type transaction::commit_async(transaction::callback on_completion) {
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::commit);
    req->status(scheduler::request_detail_status::accepted);
    req->transaction_id(transaction_id());
    log_request(*req);

    auto rctx = create_request_context(
        nullptr,
        std::make_shared<memory::lifo_paged_memory_resource>(&global::page_pool()),
        req
    );
    auto timer = std::make_shared<utils::backoff_timer>();
    auto jobid = rctx->job()->id();
    std::string txid{tx_->object()->transaction_id()};
    auto t = scheduler::create_custom_task(rctx.get(), [this, rctx, timer=std::move(timer), jobid, txid]() {
        auto res = commit_internal();
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:committing_end "
            << txid
            << " job_id:"
            << utils::hex(jobid);
        if(res == status::waiting_for_other_transaction) {
            timer->reset();
            submit_task_commit_wait(rctx.get(), [rctx, this, timer]() {
                if(! (*timer)()) return model::task_result::yield;
                auto st = tx_->object()->check_state().state_kind();
                switch(st) {
                    case ::sharksfin::TransactionState::StateKind::WAITING_CC_COMMIT:
                        return model::task_result::yield;
                    case ::sharksfin::TransactionState::StateKind::ABORTED: {
                        // get result and return error info
                        rctx->status_code(
                            status::err_aborted_retryable,
                            utils::create_abort_message(*rctx, *tx_, *database_->tables()));
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

        auto msg = res != status::ok ? utils::create_abort_message(*rctx, *tx_, *database_->tables()) : "";
        rctx->status_code(res, msg);
        scheduler::submit_teardown(*rctx);
        return model::task_result::complete;
    }, true);
    rctx->job()->callback([on_completion=std::move(on_completion), rctx, jobid, txid](){  // callback is copy-based
        VLOG(log_debug_timing_event) << "/:jogasaki:timing:committed "
            << txid
            << " job_id:"
            << utils::hex(jobid);
        on_completion(rctx->status_code(), rctx->status_message());
    });
    auto& ts = *rctx->scheduler();
    VLOG(log_debug_timing_event) << "/:jogasaki:timing:committing "
        << txid
        << " job_id:"
        << utils::hex(jobid);
    req->status(scheduler::request_detail_status::submitted);
    log_request(*req);
    ts.schedule_task(std::move(t));
    return jobid;
}

bool transaction::transaction::is_ready() const {
    auto st = tx_->object()->check_state().state_kind();
    return st != ::sharksfin::TransactionState::StateKind::WAITING_START;
}

status transaction::create_transaction(
    impl::database& db,
    std::unique_ptr<impl::transaction>& out,
    kvs::transaction_option const& options
) {
    auto ret = std::make_unique<transaction>(db);
    if(auto res = ret->init(options); res != status::ok) {
        return res;
    }
    out = std::move(ret);
    return status::ok;
}

status transaction::init(kvs::transaction_option const& options) {
    std::unique_ptr<kvs::transaction> kvs_tx{};
    if(auto res = kvs::transaction::create_transaction(*database_->kvs_db(), kvs_tx, options); res != status::ok) {
        return res;
    }
    tx_ = wrap(std::move(kvs_tx));
    return status::ok;
}

std::string_view transaction::transaction_id() const noexcept {
    return tx_->object()->transaction_id();
}

}
