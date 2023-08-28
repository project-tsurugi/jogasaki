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
#include "service.h"

#include <msgpack.hpp>
#include <google/protobuf/text_format.h>
#include <glog/logging.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/exception.h>
#include <takatori/util/string_builder.h>

#include <jogasaki/status.h>
#include <jogasaki/common.h>
#include <jogasaki/logging.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/utils/proto_field_types.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/io/record_channel_adapter.h>
#include <jogasaki/utils/decimal.h>
#include <jogasaki/utils/proto_debug_string.h>
#include <jogasaki/request_logging.h>

#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;
using takatori::util::throw_exception;
using takatori::util::string_builder;
using namespace tateyama::api::server;

constexpr static std::string_view log_location_prefix = "/:jogasaki:api:impl:service ";

namespace details {

class query_info {
public:
    using handle_parameters = std::pair<std::size_t, maybe_shared_ptr<jogasaki::api::parameter_set const>>;
    explicit query_info(std::string_view sql) :
        entity_(std::in_place_type<std::string_view>, sql)
    {}

    explicit query_info(std::size_t sid, maybe_shared_ptr<jogasaki::api::parameter_set const> params) :
        entity_(std::in_place_type<handle_parameters>, std::pair{sid, std::move(params)})
    {}

    [[nodiscard]] bool has_sql() const noexcept {
        return std::holds_alternative<std::string_view>(entity_);
    }

    [[nodiscard]] std::string_view sql() const noexcept {
        return *std::get_if<std::string_view>(std::addressof(entity_));
    }

    [[nodiscard]] std::size_t sid() const noexcept {
        return std::get_if<handle_parameters>(std::addressof(entity_))->first;
    }

    [[nodiscard]] maybe_shared_ptr<jogasaki::api::parameter_set const> const& params() const noexcept {
        return std::get_if<handle_parameters>(std::addressof(entity_))->second;
    }
private:
    std::variant<std::string_view, handle_parameters> entity_{};
};

}

template<class T>
jogasaki::api::transaction_handle validate_transaction_handle(
    T msg,
    api::database* db,
    tateyama::api::server::response& res,
    details::request_info const& req_info
);

void service::command_begin(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    std::vector<std::string> wps{};
    std::vector<std::string> rai{};
    std::vector<std::string> rae{};
    bool readonly = false;
    bool is_long = false;
    bool modifies_definitions = false;
    auto& bg = proto_req.begin();
    std::string_view label{};
    if(bg.has_option()) {
        auto& op = bg.option();
        if(op.type() == sql::request::TransactionType::READ_ONLY) {
            readonly = true;
        }
        if(op.type() == sql::request::TransactionType::LONG) {
            is_long = true;
        }
        modifies_definitions = op.modifies_definitions();
        wps.reserve(op.write_preserves().size());
        for(auto&& x : op.write_preserves()) {
            wps.emplace_back(x.table_name());
        }
        rai.reserve(op.inclusive_read_areas().size());
        for(auto&& x : op.inclusive_read_areas()) {
            rai.emplace_back(x.table_name());
        }
        rae.reserve(op.exclusive_read_areas().size());
        for(auto&& x : op.exclusive_read_areas()) {
            rae.emplace_back(x.table_name());
        }
        label = op.label();
    }
    transaction_option opts{
        readonly,
        is_long,
        std::move(wps),
        label,
        std::move(rai),
        std::move(rae),
        modifies_definitions
    };
    get_impl(*db_).do_create_transaction_async(
        [res, req_info](jogasaki::api::transaction_handle tx, status st, std::shared_ptr<api::error_info> err_info) {  //NOLINT(performance-unnecessary-value-param)
            if(st == jogasaki::status::ok) {
                details::success<sql::response::Begin>(*res, tx, req_info);
            } else {
                details::error<sql::response::Begin>(*res, err_info.get(), req_info);
            }
        },
        opts
    );
}

void service::command_prepare(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    auto& pp = proto_req.prepare();
    auto& phs = pp.placeholders();
    auto& sql = pp.sql();
    if(sql.empty()) {
        VLOG(log_error) << log_location_prefix << "missing sql";
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - missing sql",
            status::err_invalid_argument
        );
        details::error<sql::response::Prepare>(*res, err_info.get(), req_info);
        return;
    }

    std::unordered_map<std::string, jogasaki::api::field_type_kind> variables{};
    for(std::size_t i=0, n=static_cast<std::size_t>(phs.size()); i < n ; ++i) {
        auto& ph = phs.Get(static_cast<int>(i));
        variables.emplace(ph.name(), jogasaki::utils::type_for(ph.atom_type()));
    }
    jogasaki::api::statement_handle statement{};
    std::shared_ptr<error::error_info> err_info{};
    if(auto rc = get_impl(*db_).prepare(sql, variables, statement, err_info); rc == jogasaki::status::ok) {
        details::success<sql::response::Prepare>(*res, statement, req_info);
    } else {
        details::error<sql::response::Prepare>(*res, err_info.get(), req_info);
    }
}

void service::command_list_tables(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    (void) proto_req;
    std::vector<std::string> simple_names{};
    std::shared_ptr<error::error_info> err_info{};
    if(auto rc = get_impl(*db_).list_tables(simple_names, err_info); rc == jogasaki::status::ok) {
        details::success<sql::response::ListTables>(*res, simple_names, req_info);
    } else {
        details::error<sql::response::ListTables>(*res, err_info.get(), req_info);
    }
}

void service::command_get_search_path(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    (void) proto_req;
    // return empty for the time being
    details::success<sql::response::GetSearchPath>(*res, req_info);
}

void service::command_get_error_info(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    auto& gei = proto_req.get_error_info();
    auto tx = validate_transaction_handle(gei, db_, *res, req_info);
    if(! tx) {
        return;
    }

    std::shared_ptr<api::error_info> info{};
    if(auto rc = tx.error_info(info); rc != status::ok) {
        // invalid handle
        auto err_info = create_error_info(
            error_code::transaction_not_found_exception,
            "Transaction handle is invalid.",
            rc
        );
        details::error<sql::response::GetErrorInfo>(*res, err_info.get(), req_info);
    }
    details::success<sql::response::GetErrorInfo>(*res, req_info, std::move(info));
}

void service::command_dispose_transaction(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    auto& dt = proto_req.dispose_transaction();
    auto tx = validate_transaction_handle(dt, db_, *res, req_info);
    if(! tx) {
        return;
    }
    if(auto rc = db_->destroy_transaction(tx); rc != status::ok && rc != status::err_invalid_argument) {
        // unexpected error
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Unexpected error occurred in disposing transaction.",
            rc
        );
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
        return;
    }
    // err_invalid_argument means invalid tx handle, that is treated as no-op (no error)
    details::success<sql::response::ResultOnly>(*res, req_info);
}

template<class T>
jogasaki::api::transaction_handle validate_transaction_handle(
    T msg,
    api::database* db,
    tateyama::api::server::response& res,
    details::request_info const& req_info
) {
    if(! msg.has_transaction_handle()) {
        VLOG(log_error) << log_location_prefix << "missing transaction_handle";
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - missing transaction_handle",
            status::err_invalid_argument
        );
        details::error<sql::response::ResultOnly>(res, err_info.get(), req_info);
        return {};
    }
    jogasaki::api::transaction_handle tx{msg.transaction_handle().handle(), reinterpret_cast<std::uintptr_t>(db)}; //NOLINT
    if(! tx) {
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - invalid transaction handle",
            status::err_invalid_argument
        );
        details::error<sql::response::ResultOnly>(res, err_info.get(), req_info);
        return {};
    }
    return tx;
}

void abort_tx(jogasaki::api::transaction_handle tx, std::shared_ptr<error::error_info> const& err_info = {}) {
    // expecting no error from abort
    if(tx.abort() == status::err_invalid_argument) {
        return;
    }
    if(err_info) {
        auto ctx = get_transaction_context(tx);
        if(! ctx) {
            return;
        }
        ctx->error_info(err_info);
    }
}

void service::command_execute_statement(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& eq = proto_req.execute_statement();
    auto tx = validate_transaction_handle(eq, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto& sql = eq.sql();
    if(sql.empty()) {
        VLOG(log_error) << log_location_prefix << "missing sql";
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - missing sql",
            status::err_invalid_argument
        );
        abort_tx(tx, err_info);
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
        return;
    }
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    std::shared_ptr<error::error_info> err_info{};
    if(auto rc = get_impl(*db_).create_executable(sql, e, err_info); rc != jogasaki::status::ok) {
        abort_tx(tx, err_info);
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
        return;
    }
    execute_statement(res, std::shared_ptr{std::move(e)}, tx, req_info);
}

void service::command_execute_query(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& eq = proto_req.execute_query();
    auto tx = validate_transaction_handle(eq, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto& sql = eq.sql();
    if(sql.empty()) {
        VLOG(log_error) << log_location_prefix << "missing sql";
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - missing sql",
            status::err_invalid_argument
        );
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
        abort_tx(tx, err_info);
        return;
    }
    execute_query(res, details::query_info{sql}, tx, req_info);
}

template<class Response, class Request>
jogasaki::api::statement_handle validate_statement_handle(
    Request msg,
    tateyama::api::server::response& res,
    details::request_info const& req_info
) {
    if(! msg.has_prepared_statement_handle()) {
        VLOG(log_error) << log_location_prefix << "missing prepared_statement_handle";
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - missing prepared_statement_handle",
            status::err_invalid_argument
        );
        details::error<Response>(res, err_info.get(), req_info);
        return {};
    }
    jogasaki::api::statement_handle handle{msg.prepared_statement_handle().handle()};
    if (! handle) {
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - invalid prepared_statement_handle",
            status::err_invalid_argument
        );
        details::error<Response>(res, err_info.get(), req_info);
        return {};
    }
    return handle;
}

void service::command_execute_prepared_statement(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& pq = proto_req.execute_prepared_statement();
    auto tx = validate_transaction_handle(pq, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(pq, *res, req_info);
    if(! handle) {
        abort_tx(tx);
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(pq.parameters(), params);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    std::shared_ptr<error::error_info> err_info{};
    if(auto rc = get_impl(*db_).resolve(handle, std::shared_ptr{std::move(params)}, e, err_info); rc != jogasaki::status::ok) {
        abort_tx(tx, err_info);
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
        return;
    }
    execute_statement(res, std::shared_ptr{std::move(e)}, tx, req_info);
}

void service::command_execute_prepared_query(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& pq = proto_req.execute_prepared_query();
    auto tx = validate_transaction_handle(pq, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(pq, *res, req_info);
    if(! handle) {
        abort_tx(tx);
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(pq.parameters(), params);
    execute_query(res, details::query_info{handle.get(), std::shared_ptr{std::move(params)}}, tx, req_info);
}

void service::command_commit(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& cm = proto_req.commit();
    auto tx = validate_transaction_handle(cm, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto auto_dispose = cm.auto_dispose();
    tx.commit_async(
        [this, res, tx, req_info, auto_dispose](status st, std::shared_ptr<api::error_info> info) {  //NOLINT(performance-unnecessary-value-param)
            if(st == jogasaki::status::ok) {
                if(auto_dispose) {
                    if (auto rc = db_->destroy_transaction(tx); rc != jogasaki::status::ok) {
                        VLOG(log_error) << log_location_prefix << "unexpected error destroying transaction: " << rc;
                    }
                }
                details::success<sql::response::ResultOnly>(*res, req_info);
            } else {
                VLOG(log_error) << log_location_prefix << info->message();
                details::error<sql::response::ResultOnly>(*res, info.get(), req_info);
            }
        }
    );
}
void service::command_rollback(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    auto& rb = proto_req.rollback();
    auto tx = validate_transaction_handle(rb, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::rollback);
    req->transaction_id(tx.transaction_id());
    req->status(scheduler::request_detail_status::accepted);
    log_request(*req);

    if(auto rc = tx.abort(); rc == jogasaki::status::ok) {
        details::success<sql::response::ResultOnly>(*res, req_info);
    } else {
        std::shared_ptr<error::error_info> err_info{};
        if(rc == status::err_invalid_argument) {
            err_info = create_error_info(
                error_code::transaction_not_found_exception,
                "Transaction handle is invalid.",
                rc
            );
        } else {
            VLOG(log_error) << log_location_prefix << "error in transaction_->abort()";
            err_info = create_error_info(
                error_code::sql_execution_exception,
                "Unexpected error in aborting transaction.",
                rc
            );
            // currently, we assume this won't happen or the transaction is aborted anyway.
            // So let's proceed to destroy the transaction.
        }
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
    }
    req->status(scheduler::request_detail_status::finishing);
    log_request(*req);
}

void service::command_dispose_prepared_statement(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    auto& ds = proto_req.dispose_prepared_statement();

    auto handle = validate_statement_handle<sql::response::ResultOnly>(ds, *res, req_info);
    if(! handle) {
        return;
    }
    if(auto st = db_->destroy_statement(handle); st == jogasaki::status::ok) {
        details::success<sql::response::ResultOnly>(*res, req_info);
    } else {
        VLOG(log_error) << log_location_prefix << "error destroying statement";
        auto err_info = create_error_info(
            error_code::statement_not_found_exception,
            string_builder{} << "Invalid statement handle." << string_builder::to_string,
            st
        );
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
    }
}
void service::command_explain(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    auto& ex = proto_req.explain();
    auto handle = validate_statement_handle<sql::response::Explain>(ex, *res, req_info);
    if(! handle) {
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(ex.parameters(), params);

    // log explain event here to include db_->resolve duration as well as db_->explain
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::explain);
    req->statement_text(reinterpret_cast<api::impl::prepared_statement*>(handle.get())->body()->sql_text_shared());  //NOLINT
    req->status(scheduler::request_detail_status::accepted);
    log_request(*req);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    std::shared_ptr<error::error_info> err_info{};
    if(auto rc = get_impl(*db_).resolve(handle, std::shared_ptr{std::move(params)}, e, err_info);
        rc != jogasaki::status::ok) {
        details::error<sql::response::Explain>(*res, err_info.get(), req_info);
        req->status(scheduler::request_detail_status::finishing);
        log_request(*req, false);
        return;
    }
    std::stringstream ss{};
    if (auto st = db_->explain(*e, ss); st == jogasaki::status::ok) {
        details::success<sql::response::Explain>(*res, ss.str(), e->meta(), req_info);
    } else {
        throw_exception(std::logic_error{"explain failed"});
    }

    req->status(scheduler::request_detail_status::finishing);
    log_request(*req);
}

//TODO put global constant file
constexpr static std::size_t max_records_per_file = 10000;

void service::command_execute_dump(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& ed = proto_req.execute_dump();
    auto tx = validate_transaction_handle(ed, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(ed, *res, req_info);
    if(! handle) {
        return;
    }

    auto params = jogasaki::api::create_parameter_set();
    set_params(ed.parameters(), params);

    dump_option opts{};
    opts.max_records_per_file_ = (ed.has_option() && ed.option().max_record_count_per_file() > 0) ?
        ed.option().max_record_count_per_file() :
        max_records_per_file;
    opts.keep_files_on_error_ = ed.has_option() && ed.option().fail_behavior() == proto::sql::request::KEEP_FILES;
    execute_dump(res, details::query_info{handle.get(), std::shared_ptr{std::move(params)}}, tx, ed.directory(), opts, req_info);
}

void service::command_execute_load(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& ed = proto_req.execute_load();
    jogasaki::api::transaction_handle tx{};
    if(ed.has_transaction_handle()) {
        tx = validate_transaction_handle(ed, db_, *res, req_info);
        if (!tx) {
            return;
        }
    } else {
        // non-transactional load
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(ed, *res, req_info);
    if(! handle) {
        return;
    }

    auto params = jogasaki::api::create_parameter_set();
    set_params(ed.parameters(), params);
    auto list = ed.file();
    std::vector<std::string> files{};
    for(auto&& f : list) {
        files.emplace_back(f);
    }
    execute_load(res, details::query_info{handle.get(), std::shared_ptr{std::move(params)}}, tx, files, req_info);
}

void service::command_describe_table(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::request_info const& req_info
) {
    auto& dt = proto_req.describe_table();

    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::describe_table);
    req->status(scheduler::request_detail_status::accepted);

    log_request(*req);
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    auto table = db_->find_table(dt.name());
    if(! table) {
        VLOG(log_error) << log_location_prefix << "table not found : " << dt.name();
        auto st = status::err_not_found;
        auto err_info = create_error_info(
            error_code::target_not_found_exception,
            string_builder{} << "Target table \"" << dt.name() << "\" is not found." << string_builder::to_string,
            st
        );
        details::error<sql::response::DescribeTable>(*res, err_info.get(), req_info);
        req->status(scheduler::request_detail_status::finishing);
        log_request(*req, false);
        return;
    }
    details::success<sql::response::DescribeTable>(*res, table.get(), req_info);

    req->status(scheduler::request_detail_status::finishing);
    log_request(*req);
}

bool service::operator()(
    std::shared_ptr<tateyama::api::server::request const> req,  //NOLINT(performance-unnecessary-value-param)
    std::shared_ptr<tateyama::api::server::response> res  //NOLINT(performance-unnecessary-value-param)
) {
    try {
        return process(std::move(req), std::move(res));
    } catch (std::exception& e) {
        LOG(ERROR) << log_location_prefix << "Unhandled exception caught: " << e.what();
        if(auto* tr = takatori::util::find_trace(e); tr != nullptr) {
            LOG(ERROR) << log_location_prefix << *tr;
        }
    }
    return true;
}

bool service::process(
    std::shared_ptr<tateyama::api::server::request const> req,  //NOLINT(performance-unnecessary-value-param)
    std::shared_ptr<tateyama::api::server::response> res  //NOLINT(performance-unnecessary-value-param)
) {
    std::size_t reqid = request_id_src_++;
    details::request_info req_info{reqid};
    sql::request::Request proto_req{};
    thread_local std::atomic_size_t cnt = 0;
    bool enable_performance_counter = false;
    if (++cnt > 0 && cnt % 1000 == 0) { // measure with performance counter on every 1000 invocations
        enable_performance_counter = true;
        LIKWID_MARKER_START("service");
    }
    if(req->session_id() != 0) {
        // TODO temporary fix : not to send back header if request doesn't add session_id, which means legacy request
        res->session_id(req->session_id());
    }
    {
        trace_scope_name("parse_request");  //NOLINT
        auto s = req->payload();
        if (!proto_req.ParseFromArray(s.data(), static_cast<int>(s.size()))) {
            VLOG(log_error) << log_location_prefix << "parse error";
            res->code(response_code::io_error);
            std::string msg{"parse error with request body"};
            VLOG(log_trace) << log_location_prefix << "respond with body (rid=" << reqid << " len=" << msg.size() << "): " << msg;
            res->body(msg);
            return true;
        }
        VLOG(log_trace) << log_location_prefix << "request received (rid=" << reqid << " len=" << s.size() << "): " << utils::to_debug_string(proto_req);
    }

    switch (proto_req.request_case()) {
        case sql::request::Request::RequestCase::kBegin: {
            trace_scope_name("cmd-begin");  //NOLINT
            command_begin(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kPrepare: {
            trace_scope_name("cmd-prepare");  //NOLINT
            command_prepare(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kExecuteStatement: {
            trace_scope_name("cmd-execute_statement");  //NOLINT
            command_execute_statement(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kExecuteQuery: {
            trace_scope_name("cmd-execute_query");  //NOLINT
            command_execute_query(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kExecutePreparedStatement: {
            trace_scope_name("cmd-execute_prepared_statement");  //NOLINT
            command_execute_prepared_statement(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kExecutePreparedQuery: {
            trace_scope_name("cmd-execute_prepared_query");  //NOLINT
            command_execute_prepared_query(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kCommit: {
            trace_scope_name("cmd-commit");  //NOLINT
            command_commit(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kRollback: {
            trace_scope_name("cmd-rollback");  //NOLINT
            command_rollback(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kDisposePreparedStatement: {
            trace_scope_name("cmd-dispose_prepared_statement");  //NOLINT
            command_dispose_prepared_statement(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kExplain: {
            trace_scope_name("cmd-explain");  //NOLINT
            command_explain(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kExecuteDump: {
            trace_scope_name("cmd-dump");  //NOLINT
            command_execute_dump(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kExecuteLoad: {
            trace_scope_name("cmd-load");  //NOLINT
            command_execute_load(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kDescribeTable: {
            trace_scope_name("cmd-describe_table");  //NOLINT
            command_describe_table(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kListTables: {
            trace_scope_name("cmd-list_tables");  //NOLINT
            command_list_tables(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kGetSearchPath: {
            trace_scope_name("cmd-get_search_path");  //NOLINT
            command_get_search_path(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kGetErrorInfo: {
            trace_scope_name("cmd-get_error_info");  //NOLINT
            command_get_error_info(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kDisposeTransaction: {
            trace_scope_name("cmd-dispose_transaction");  //NOLINT
            command_dispose_transaction(proto_req, res, req_info);
            break;
        }
        default:
            std::string msg{"invalid request code: "};
            VLOG(log_error) << log_location_prefix << msg << proto_req.request_case();
            res->code(response_code::io_error);
            VLOG(log_trace) << log_location_prefix << "respond with body (rid=" << reqid << " len=" << msg.size() << "):" << std::endl << msg;
            res->body(msg);
            break;
    }
    if (enable_performance_counter) {
        LIKWID_MARKER_STOP("service");
    }
    return true;
}

void service::execute_statement(
    std::shared_ptr<tateyama::api::server::response> const& res,
    std::shared_ptr<jogasaki::api::executable_statement> stmt,
    jogasaki::api::transaction_handle tx,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto c = std::make_shared<callback_control>(res);
    auto* cbp = c.get();
    auto cid = c->id_;
    if(! callbacks_.emplace(cid, std::move(c))) {
        throw_exception(std::logic_error{"callback already exists"});
    }
    if(auto success = tx.execute_async(
            std::move(stmt),
            [cbp, this, req_info](status s, std::shared_ptr<api::error_info> info){  //NOLINT(performance-unnecessary-value-param)
                if (s == jogasaki::status::ok) {
                    details::success<sql::response::ResultOnly>(*cbp->response_, req_info);
                } else {
                    details::error<sql::response::ResultOnly>(*cbp->response_, info.get(), req_info);
                }
                if(! callbacks_.erase(cbp->id_)) {
                    throw_exception(std::logic_error{"missing callback"});
                }
            }
        );! success) {
        // normally this should not happen
        throw_exception(std::logic_error{"execute_async failed"});
    }
}

takatori::decimal::triple to_triple(::jogasaki::proto::sql::common::Decimal const& arg) {
    std::string_view buf{arg.unscaled_value()};
    auto exp = arg.exponent();
    return utils::read_decimal(buf, -exp);
}

void service::set_params(::google::protobuf::RepeatedPtrField<sql::request::Parameter> const& ps, std::unique_ptr<jogasaki::api::parameter_set>& params) {
    for (std::size_t i=0, n=static_cast<std::size_t>(ps.size()); i < n; ++i) {
        auto& p = ps.Get(static_cast<int>(i));
        switch (p.value_case()) {
            case sql::request::Parameter::ValueCase::kInt4Value:
                params->set_int4(p.name(), p.int4_value());
                break;
            case sql::request::Parameter::ValueCase::kInt8Value:
                params->set_int8(p.name(), p.int8_value());
                break;
            case sql::request::Parameter::ValueCase::kFloat4Value:
                params->set_float4(p.name(), p.float4_value());
                break;
            case sql::request::Parameter::ValueCase::kFloat8Value:
                params->set_float8(p.name(), p.float8_value());
                break;
            case sql::request::Parameter::ValueCase::kCharacterValue:
                params->set_character(p.name(), p.character_value());
                break;
            case sql::request::Parameter::ValueCase::kDecimalValue:
                params->set_decimal(p.name(), to_triple(p.decimal_value()));
                break;
            case sql::request::Parameter::ValueCase::kDateValue:
                params->set_date(p.name(), field_type_traits<kind::date>::runtime_type{p.date_value()});
                break;
            case sql::request::Parameter::ValueCase::kTimeOfDayValue:
                params->set_time_of_day(p.name(), field_type_traits<kind::time_of_day>::runtime_type{std::chrono::duration<std::uint64_t, std::nano>{p.time_of_day_value()}});
                break;
            case sql::request::Parameter::ValueCase::kTimePointValue: {
                auto& v = p.time_point_value();
                params->set_time_point(p.name(), field_type_traits<kind::time_point>::runtime_type{
                    std::chrono::duration<std::int64_t>{v.offset_seconds()},
                    std::chrono::nanoseconds{v.nano_adjustment()}
                });
                break;
            }
            case sql::request::Parameter::ValueCase::kTimeOfDayWithTimeZoneValue:
                // TODO pass time zone offset
                params->set_time_of_day(p.name(), field_type_traits<kind::time_of_day>::runtime_type{
                    std::chrono::duration<std::uint64_t, std::nano>{
                        p.time_of_day_with_time_zone_value().offset_nanoseconds()
                    }
                });
                break;
            case sql::request::Parameter::ValueCase::kTimePointWithTimeZoneValue: {
                // TODO pass time zone offset
                auto& v = p.time_point_with_time_zone_value();
                params->set_time_point(p.name(), field_type_traits<kind::time_point>::runtime_type{
                    std::chrono::duration<std::int64_t>{v.offset_seconds()},
                    std::chrono::nanoseconds{v.nano_adjustment()}
                });
                break;
            }
            case sql::request::Parameter::ValueCase::kReferenceColumnPosition:
                params->set_reference_column(p.name(), p.reference_column_position());
                break;
            case sql::request::Parameter::ValueCase::kReferenceColumnName:
                params->set_reference_column(p.name(), p.reference_column_name());
                break;
            default:
                params->set_null(p.name());
                break;
        }
    }
}

void service::execute_query(
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::query_info const& q,
    jogasaki::api::transaction_handle tx,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    BOOST_ASSERT(tx);  //NOLINT
    auto c = std::make_shared<callback_control>(res);
    auto& info = c->channel_info_;
    info = std::make_unique<details::channel_info>();
    info->name_ = std::string("resultset-");
    info->name_ += std::to_string(new_resultset_id());

    bool has_result_records = false;
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    std::shared_ptr<error::error_info> err_info{};
    if(q.has_sql()) {
        if(auto rc = get_impl(*db_).create_executable(q.sql(), e, err_info); rc != jogasaki::status::ok) {
            VLOG(log_error) << log_location_prefix << "error in db_->create_executable() : " << rc;
            details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
            return;
        }
        has_result_records = e->meta() != nullptr;
    } else {
        jogasaki::api::statement_handle statement{q.sid()};
        if(auto rc = get_impl(*db_).resolve(statement, q.params(), e, err_info); rc != jogasaki::status::ok) {
            details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
            return;
        }
        has_result_records = statement.has_result_records();
    }
    if(! has_result_records) {
        auto msg = "statement has no result records, but called with API expecting result records";
        VLOG(log_error) << log_location_prefix << msg;
        auto err_info = create_error_info(error_code::inconsistent_statement_exception, msg, status::err_illegal_operation);
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
        return;
    }

    std::shared_ptr<tateyama::api::server::data_channel> ch{};
    {
        trace_scope_name("acquire_channel");  //NOLINT
        if(auto rc = res->acquire_channel(info->name_, ch); rc != tateyama::status::ok) {
            throw_exception(std::logic_error{"acquire_channel failed"});
        }
    }
    info->data_channel_ = std::make_shared<jogasaki::api::impl::data_channel>(std::move(ch));
    info->meta_ = e->meta();
    details::send_body_head(*res, *info, req_info);
    auto* cbp = c.get();
    auto cid = c->id_;
    callbacks_.emplace(cid, std::move(c));
    if(auto rc = tx.execute_async(
            std::shared_ptr{std::move(e)},
            info->data_channel_,
            [cbp, this, req_info](status s, std::shared_ptr<api::error_info> info){  //NOLINT(performance-unnecessary-value-param)
                {
                    trace_scope_name("release_channel");  //NOLINT
                    cbp->response_->release_channel(*cbp->channel_info_->data_channel_->origin());
                }
                if (s == jogasaki::status::ok) {
                    details::success<sql::response::ResultOnly>(*cbp->response_, req_info);
                } else {
                    details::error<sql::response::ResultOnly>(*cbp->response_, info.get(), req_info);
                }
                if(! callbacks_.erase(cbp->id_)) {
                    throw_exception(std::logic_error{"missing callback"});
                }
            }
        ); ! rc) {
        // for now execute_async doesn't raise error. But if it happens in future, error response should be sent here.
        throw_exception(std::logic_error{"execute_async failed"});
    }
}

std::size_t service::new_resultset_id() const noexcept {
    static std::atomic_size_t resultset_id{};
    return ++resultset_id;
}

bool service::shutdown(bool force) {
    (void) force;
    // db should be shutdown by resource
    LIKWID_MARKER_CLOSE;
    return true;
}

void details::reply(
    tateyama::api::server::response& res,
    sql::response::Response& r,
    details::request_info const& req_info,
    bool body_head
) {
    std::string ss{};
    if (!r.SerializeToString(&ss)) {
        throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    if (body_head) {
        trace_scope_name("body_head");  //NOLINT
        VLOG(log_trace) << log_location_prefix << "respond with body_head (rid=" << req_info.id() << " len=" << ss.size() << "): " << utils::to_debug_string(r);
        res.body_head(ss);
        return;
    }
    {
        trace_scope_name("body");  //NOLINT
        VLOG(log_trace) << log_location_prefix << "respond with body (rid=" << req_info.id() << " len=" << ss.size() << "): " << utils::to_debug_string(r);
        res.body(ss);
    }
}

template
void details::set_metadata(jogasaki::api::record_meta const* metadata, sql::response::ResultSetMetadata& meta);
template
void details::set_metadata(jogasaki::api::record_meta const* metadata, sql::response::Explain::Success& meta);

template <class T>
void details::set_metadata(jogasaki::api::record_meta const* metadata, T& meta) {
    if(metadata == nullptr) return;
    std::size_t n = metadata->field_count();
    for (std::size_t i = 0; i < n; i++) {
        auto column = meta.add_columns();
        if(auto name = metadata->field_name(i); name.has_value()) {
            column->set_name(std::string{*name});
        }
        auto& fld = metadata->at(i);
        switch(fld.kind()) {
            case jogasaki::api::field_type_kind::boolean:
                column->set_atom_type(sql::common::AtomType::BOOLEAN);
                break;
            case jogasaki::api::field_type_kind::int4:
                column->set_atom_type(sql::common::AtomType::INT4);
                break;
            case jogasaki::api::field_type_kind::int8:
                column->set_atom_type(sql::common::AtomType::INT8);
                break;
            case jogasaki::api::field_type_kind::float4:
                column->set_atom_type(sql::common::AtomType::FLOAT4);
                break;
            case jogasaki::api::field_type_kind::float8:
                column->set_atom_type(sql::common::AtomType::FLOAT8);
                break;
            case jogasaki::api::field_type_kind::character:
                column->set_atom_type(sql::common::AtomType::CHARACTER);
                break;
            case jogasaki::api::field_type_kind::decimal:
                column->set_atom_type(sql::common::AtomType::DECIMAL);
                break;
            case jogasaki::api::field_type_kind::date:
                column->set_atom_type(sql::common::AtomType::DATE);
                break;
            case jogasaki::api::field_type_kind::time_of_day:
                BOOST_ASSERT(fld.time_of_day_option() != nullptr);  //NOLINT
                if(fld.time_of_day_option()->with_offset()) {
                    column->set_atom_type(sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE);
                    break;
                }
                column->set_atom_type(sql::common::AtomType::TIME_OF_DAY);
                break;
            case jogasaki::api::field_type_kind::time_point:
                BOOST_ASSERT(fld.time_point_option() != nullptr);  //NOLINT
                if(fld.time_point_option()->with_offset()) {
                    column->set_atom_type(sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE);
                    break;
                }
                column->set_atom_type(sql::common::AtomType::TIME_POINT);
                break;
            default:
                LOG(ERROR) << log_location_prefix << "unsupported data type at field (" << i << "): " << metadata->at(i).kind();
                break;
        }
    }
}

void service::execute_dump(
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::query_info const& q,
    jogasaki::api::transaction_handle tx,
    std::string_view directory,
    dump_option const& opts,
    details::request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    BOOST_ASSERT(tx);  //NOLINT
    auto c = std::make_shared<callback_control>(res);
    auto& info = c->channel_info_;
    info = std::make_unique<details::channel_info>();
    info->name_ = std::string("resultset-");
    info->name_ += std::to_string(new_resultset_id());

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    BOOST_ASSERT(! q.has_sql());  //NOLINT
    jogasaki::api::statement_handle statement{q.sid()};
    std::shared_ptr<error::error_info> err_info{};
    if(auto rc = get_impl(*db_).resolve(statement, q.params(), e, err_info); rc != jogasaki::status::ok) {
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
        return;
    }

    std::shared_ptr<tateyama::api::server::data_channel> ch{};
    {
        trace_scope_name("acquire_channel");  //NOLINT
        if(auto rc = res->acquire_channel(info->name_, ch); rc != tateyama::status::ok) {
            throw_exception(std::logic_error{"acquire_channel failed"});
        }
    }
    info->data_channel_ = std::make_shared<jogasaki::api::impl::data_channel>(ch);
    {
        auto m = std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                meta::field_type(meta::field_enum_tag<meta::field_type_kind::character>),
            },
            boost::dynamic_bitset<std::uint64_t>{1}.flip()
        );
        api::impl::record_meta meta{
            std::make_shared<meta::external_record_meta>(m, std::vector<std::optional<std::string>>{"file_name"} )
        };
        info->meta_ = &meta;
        details::send_body_head(*res, *info, req_info);
    }

    auto* cbp = c.get();
    auto cid = c->id_;
    callbacks_.emplace(cid, std::move(c));
    auto t = get_impl(*db_).find_transaction(tx);
    if(auto rc = executor::execute_dump(  //NOLINT
            get_impl(*db_),
            t,
            std::shared_ptr{std::move(e)},
            info->data_channel_,
            directory,
            [cbp, this, req_info](status s, std::shared_ptr<error::error_info> info) {  //NOLINT(performance-unnecessary-value-param)
                {
                    trace_scope_name("release_channel");  //NOLINT
                    cbp->response_->release_channel(*cbp->channel_info_->data_channel_->origin());
                }
                if (s == jogasaki::status::ok) {
                    details::success<sql::response::ResultOnly>(*cbp->response_, req_info);
                } else {
                    details::error<sql::response::ResultOnly>(*cbp->response_, info.get(), req_info);
                }
                if(! callbacks_.erase(cbp->id_)) {
                    throw_exception(std::logic_error{"missing callback"});
                }
            },
            opts.max_records_per_file_ == 0 ? max_records_per_file : opts.max_records_per_file_,
            opts.keep_files_on_error_
    ); ! rc) {
        // for now execute_async doesn't raise error. But if it happens in future, error response should be sent here.
        throw_exception(std::logic_error{"execute_dump failed"});
    }
}

void service::execute_load( //NOLINT
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::query_info const& q,
    jogasaki::api::transaction_handle tx,
    std::vector<std::string> const& files,
    details::request_info const& req_info
) {
    for(auto&& f : files) {
        VLOG(log_info) << log_location_prefix << "load processing file: " << f;
    }
    BOOST_ASSERT(! q.has_sql());  //NOLINT
    jogasaki::api::statement_handle statement{q.sid()};

    auto c = std::make_shared<callback_control>(res);
    auto* cbp = c.get();
    auto cid = c->id_;
    if(! callbacks_.emplace(cid, std::move(c))) {
        throw_exception(std::logic_error{"callback already exists"});
    }
    if(tx) {
        auto t = get_impl(*db_).find_transaction(tx);
        if (auto rc = executor::execute_load(  //NOLINT
                get_impl(*db_),
                t,
                statement,
                q.params(),
                files,
                [cbp, this, req_info](status s, std::shared_ptr<error::error_info> info) {  //NOLINT(performance-unnecessary-value-param)
                    if (s == jogasaki::status::ok) {
                        details::success<sql::response::ResultOnly>(*cbp->response_, req_info);
                    } else {
                        details::error<sql::response::ResultOnly>(*cbp->response_, info.get(), req_info);
                    }
                    if (!callbacks_.erase(cbp->id_)) {
                        throw_exception(std::logic_error{"missing callback"});
                    }
                }
            ); !rc) {
            // for now execute_async doesn't raise error. But if it happens in future, error response should be sent here.
            throw_exception(std::logic_error{"execute_load failed"});
        }
    } else {
        //non transactional load
        if (auto rc = get_impl(*db_).execute_load(  //NOLINT
                statement,
                q.params(),
                files,
                [cbp, this, req_info](status s, std::shared_ptr<error::error_info> err_info) {  //NOLINT(performance-unnecessary-value-param)
                    if (s == jogasaki::status::ok) {
                        details::success<sql::response::ResultOnly>(*cbp->response_, req_info);
                    } else {
                        details::error<sql::response::ResultOnly>(*cbp->response_, err_info.get(), req_info);
                    }
                    if (!callbacks_.erase(cbp->id_)) {
                        throw_exception(std::logic_error{"missing callback"});
                    }
                }
            ); !rc) {
            // for now execute_async doesn't raise error. But if it happens in future, error response should be sent here.
            throw_exception(std::logic_error{"execute_load failed"});
        }
    }
}

service::service(std::shared_ptr<tateyama::api::configuration::whole> cfg, jogasaki::api::database* db) :
    cfg_(std::move(cfg)),
    db_(db)
{}

bool service::start() {
    // db should be started by resource
    return true;
}

jogasaki::api::database* service::database() const noexcept {
    return db_;
}
}
