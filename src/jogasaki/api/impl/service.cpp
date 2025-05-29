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
#include "service.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <exception>
#include <optional>
#include <ostream>
#include <ratio>
#include <stdexcept>
#include <type_traits>
#include <unordered_map>
#include <variant>
#include <boost/cstdint.hpp>
#include <boost/dynamic_bitset/dynamic_bitset.hpp>
#include <glog/logging.h>

#include <takatori/decimal/triple.h>
#include <takatori/util/exception.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/stacktrace.h>
#include <takatori/util/string_builder.h>
#include <tateyama/api/server/data_channel.h>
#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/api/server/writer.h>
#include <tateyama/common.h>
#include <tateyama/proto/diagnostics.pb.h>
#include <tateyama/status.h>

#include <jogasaki/api/commit_option.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/field_type.h>
#include <jogasaki/api/field_type_kind.h>
#include <jogasaki/api/field_type_traits.h>
#include <jogasaki/api/impl/data_channel.h>
#include <jogasaki/api/impl/database.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/statement_handle_internal.h>
#include <jogasaki/api/time_of_day_field_option.h>
#include <jogasaki/api/time_point_field_option.h>
#include <jogasaki/api/transaction_handle.h>
#include <jogasaki/api/transaction_handle_internal.h>
#include <jogasaki/api/transaction_option.h>
#include <jogasaki/commit_response.h>
#include <jogasaki/common.h>
#include <jogasaki/configuration.h>
#include <jogasaki/constants.h>
#include <jogasaki/datastore/get_lob_data.h>
#include <jogasaki/error/error_info.h>
#include <jogasaki/error/error_info_factory.h>
#include <jogasaki/executor/executor.h>
#include <jogasaki/executor/file/time_unit_kind.h>
#include <jogasaki/executor/io/dump_config.h>
#include <jogasaki/logging.h>
#include <jogasaki/meta/character_field_option.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/meta/field_type.h>
#include <jogasaki/meta/record_meta.h>
#include <jogasaki/plan/prepared_statement.h>
#include <jogasaki/request_info.h>
#include <jogasaki/request_logging.h>
#include <jogasaki/scheduler/request_detail.h>
#include <jogasaki/status.h>
#include <jogasaki/transaction_context.h>
#include <jogasaki/utils/binary_printer.h>
#include <jogasaki/utils/convert_offset.h>
#include <jogasaki/utils/decimal.h>
#include <jogasaki/utils/proto_debug_string.h>
#include <jogasaki/utils/proto_field_types.h>
#include <jogasaki/utils/string_manipulation.h>

namespace jogasaki::api::impl {

using takatori::util::maybe_shared_ptr;
using takatori::util::throw_exception;
using takatori::util::string_builder;
using namespace tateyama::api::server;

constexpr static std::string_view log_location_prefix = "/:jogasaki:api:impl:service ";

namespace details {

void report_error(
    tateyama::api::server::response& res,
    tateyama::proto::diagnostics::Code code,
    std::string_view msg,
    std::size_t reqid
) {
    VLOG(log_error) << log_location_prefix << msg;
    tateyama::proto::diagnostics::Record rec{};
    rec.set_code(code);
    rec.set_message(msg.data(), msg.size());
    VLOG(log_trace) << log_location_prefix << "respond with error (rid=" << reqid
                    << "): " << utils::to_debug_string(rec);
    res.error(rec);
}

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

}  // namespace details

template<class Response, class Request>
jogasaki::api::transaction_handle validate_transaction_handle(
    Request msg,
    api::database* db,
    tateyama::api::server::response& res,
    request_info const& req_info
);

transaction_type_kind from(sql::request::TransactionType arg) {
    using t = sql::request::TransactionType;
    switch (arg) {
        case t::SHORT: return transaction_type_kind::occ;
        case t::LONG: return transaction_type_kind::ltx;
        case t::READ_ONLY: return transaction_type_kind::rtx;
        default: return transaction_type_kind::unknown;
    }
    std::abort();
}

void service::command_begin(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    std::vector<std::string> wps{};
    std::vector<std::string> rai{};
    std::vector<std::string> rae{};
    transaction_type_kind type{};
    bool modifies_definitions = false;
    auto& bg = proto_req.begin();
    std::string_view label{};
    std::optional<std::uint32_t> scan_parallel{};
    if(bg.has_option()) {
        auto& op = bg.option();
        type = from(op.type());
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
        if (op.scan_parallel_opt_case() == sql::request::TransactionOption::kScanParallel) {
            scan_parallel = op.scan_parallel();
        }
    }
    transaction_option opts{
        type,
        std::move(wps),
        label,
        std::move(rai),
        std::move(rae),
        modifies_definitions,
        scan_parallel,
        req_info.request_source() ? std::optional<std::size_t>{req_info.request_source()->session_id()} : std::nullopt
    };
    get_impl(*db_).do_create_transaction_async(
        [res, req_info](jogasaki::api::transaction_handle tx, status st, std::shared_ptr<api::error_info> err_info) {  //NOLINT(performance-unnecessary-value-param)
            if(st == jogasaki::status::ok) {
                details::success<sql::response::Begin>(*res, tx, req_info);
            } else {
                details::error<sql::response::Begin>(*res, err_info.get(), req_info);
            }
        },
        opts,
        req_info
    );
}

void service::command_prepare(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
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
        auto t = jogasaki::utils::type_for(ph.atom_type());
        if(t == jogasaki::api::field_type_kind::undefined) {
            auto err_info = create_error_info(
                error_code::sql_execution_exception,
                string_builder{} << "invalid place holder type:" << AtomType_Name(ph.atom_type()) << string_builder::to_string,
                status::err_invalid_argument
            );
            details::error<sql::response::Prepare>(*res, err_info.get(), req_info);
            return;
        }
        variables.emplace(ph.name(), t);
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
    request_info const& req_info
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
    request_info const& req_info
) {
    (void) proto_req;
    // return empty for the time being
    details::success<sql::response::GetSearchPath>(*res, req_info);
}

void service::command_get_error_info(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    auto& gei = proto_req.get_error_info();
    auto tx = validate_transaction_handle<sql::response::GetErrorInfo>(gei, db_, *res, req_info);
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
        return;
    }
    details::success<sql::response::GetErrorInfo>(*res, req_info, std::move(info));
}

void service::command_dispose_transaction(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    auto& dt = proto_req.dispose_transaction();
    auto tx = validate_transaction_handle<sql::response::ResultOnly>(dt, db_, *res, req_info);
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

template<class Response, class Request>
jogasaki::api::transaction_handle validate_transaction_handle(
    Request msg,
    api::database* db,
    tateyama::api::server::response& res,
    request_info const& req_info
) {
    (void) db;
    if(! msg.has_transaction_handle()) {
        VLOG(log_error) << log_location_prefix << "missing transaction_handle";
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - missing transaction_handle",
            status::err_invalid_argument
        );
        details::error<Response>(res, err_info.get(), req_info);
        return {};
    }
    api::transaction_handle tx{
        msg.transaction_handle().handle(),
        req_info.request_source() ? std::optional<std::size_t>{req_info.request_source()->session_id()} : std::nullopt
    };
    if(! tx) {
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - invalid transaction handle",
            status::err_invalid_argument
        );
        details::error<Response>(res, err_info.get(), req_info);
        return {};
    }
    return tx;
}

template<class Request>
std::string extract_transaction(
    Request msg,
    api::database* db,
    std::shared_ptr<error::error_info>& err_info,
    request_info const& req_info
) {
    (void) db;
    if(! msg.has_transaction_handle()) {
        err_info = create_error_info(
            error_code::sql_execution_exception,
            "Invalid request format - missing transaction_handle",
            status::err_invalid_argument
        );
        return {};
    }
    api::transaction_handle tx{
        msg.transaction_handle().handle(),
        req_info.request_source() ? std::optional<std::size_t>{req_info.request_source()->session_id()} : std::nullopt
    };
    auto t = get_transaction_context(tx);
    if(! t) {
        // failed to get transaction_context
        // this is not an error because depending on the timing transaction may be disposed
        // return empty string as transaction id
        return {};
    }
    return std::string{t->transaction_id()};
}

void abort_transaction(
    jogasaki::api::transaction_handle tx,
    request_info const& req_info,
    std::shared_ptr<error::error_info> const& err_info = {}) {
    // expecting no error from abort
    if(tx.abort_transaction(req_info) == status::err_invalid_argument) {
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
    request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& eq = proto_req.execute_statement();
    auto tx = validate_transaction_handle<sql::response::ExecuteResult>(eq, db_, *res, req_info);
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
        abort_transaction(tx, req_info, err_info);
        details::error<sql::response::ExecuteResult>(*res, err_info.get(), req_info);
        return;
    }
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    std::shared_ptr<error::error_info> err_info{};
    if(auto rc = get_impl(*db_).create_executable(sql, e, err_info); rc != jogasaki::status::ok) {
        abort_transaction(tx, req_info, err_info);
        details::error<sql::response::ExecuteResult>(*res, err_info.get(), req_info);
        return;
    }
    execute_statement(res, std::shared_ptr{std::move(e)}, tx, req_info);
}

void service::command_execute_query(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& eq = proto_req.execute_query();
    auto tx = validate_transaction_handle<sql::response::ResultOnly>(eq, db_, *res, req_info);
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
        abort_transaction(tx, req_info, err_info);
        return;
    }
    execute_query(res, details::query_info{sql}, tx, req_info);
}

template<class Response, class Request>
jogasaki::api::statement_handle validate_statement_handle(
    Request msg,
    api::database* db,
    tateyama::api::server::response& res,
    request_info const& req_info
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
    jogasaki::api::statement_handle handle{msg.prepared_statement_handle().handle(), reinterpret_cast<std::uintptr_t>(db)}; //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
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
    request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& pq = proto_req.execute_prepared_statement();
    auto tx = validate_transaction_handle<sql::response::ExecuteResult>(pq, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ExecuteResult>(pq, db_, *res, req_info);
    if(! handle) {
        abort_transaction(tx, req_info);
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(pq.parameters(), params, req_info);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    std::shared_ptr<error::error_info> err_info{};
    if(auto rc = get_impl(*db_).resolve(handle, std::shared_ptr{std::move(params)}, e, err_info);
       rc != jogasaki::status::ok) {
        abort_transaction(tx, req_info, err_info);
        details::error<sql::response::ExecuteResult>(*res, err_info.get(), req_info);
        return;
    }
    execute_statement(res, std::shared_ptr{std::move(e)}, tx, req_info);
}

void service::command_execute_prepared_query(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& pq = proto_req.execute_prepared_query();
    auto tx = validate_transaction_handle<sql::response::ResultOnly>(pq, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(pq, db_, *res, req_info);
    if(! handle) {
        abort_transaction(tx, req_info);
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(pq.parameters(), params, req_info);
    execute_query(res, details::query_info{handle.get(), std::shared_ptr{std::move(params)}}, tx, req_info);
}

commit_response_kind from(::jogasaki::proto::sql::request::CommitStatus st) {
    using cs = ::jogasaki::proto::sql::request::CommitStatus;
    switch(st) {
        case cs::ACCEPTED: return commit_response_kind::accepted;
        case cs::AVAILABLE: return commit_response_kind::available;
        case cs::STORED: return commit_response_kind::stored;
        case cs::PROPAGATED: return commit_response_kind::propagated;
        default: return commit_response_kind::undefined;
    }
    std::abort();
}

void service::command_commit(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& cm = proto_req.commit();
    auto tx = validate_transaction_handle<sql::response::ResultOnly>(cm, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto nt = from(cm.has_option() ? cm.option().notification_type() : cm.notification_type());
    auto cr = nt != commit_response_kind::undefined ? nt : db_->config()->default_commit_response();
    commit_response_kind_set responses{};
    if(cr == commit_response_kind::accepted || cr == commit_response_kind::available) {
        // currently accepted and available are treated the same
        responses.insert(commit_response_kind::accepted);
        cr = commit_response_kind::accepted;
    }
    if(cr == commit_response_kind::stored || cr == commit_response_kind::propagated) {
        responses.insert(commit_response_kind::stored);
        cr = commit_response_kind::stored;
    }

    commit_option opt{};
    opt.auto_dispose_on_success(cm.has_option() ? cm.option().auto_dispose() : cm.auto_dispose()).commit_response(cr);

    auto tctx = get_transaction_context(tx);
    executor::commit_async(
        get_impl(*db_),
        std::move(tctx),
        [res, req_info](commit_response_kind) {
            // for now, callback does same regardless of kind
            details::success<sql::response::ResultOnly>(*res, req_info);
        },
        commit_response_kind_set{opt.commit_response()},
        [res, req_info](commit_response_kind, status, std::shared_ptr<error::error_info> info) { //NOLINT(performance-unnecessary-value-param)
            // for now, callback does same regardless of kind
            VLOG(log_error) << log_location_prefix << info->message();
            details::error<sql::response::ResultOnly>(*res, info.get(), req_info);
        },
        opt,
        req_info
    );
}

void service::command_rollback(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    auto& rb = proto_req.rollback();
    auto tx = validate_transaction_handle<sql::response::ResultOnly>(rb, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::rollback);
    req->transaction_id(tx.transaction_id());
    req->status(scheduler::request_detail_status::accepted);
    log_request(*req);

    if(auto rc = tx.abort_transaction(req_info); rc == jogasaki::status::ok) {
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
            VLOG(log_error) << log_location_prefix << "error in transaction_->abort_transaction()";
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
    request_info const& req_info
) {
    auto& ds = proto_req.dispose_prepared_statement();

    auto handle = validate_statement_handle<sql::response::ResultOnly>(ds, db_, *res, req_info);
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
    request_info const& req_info
) {
    auto& ex = proto_req.explain();
    auto handle = validate_statement_handle<sql::response::Explain>(ex, db_, *res, req_info);
    if(! handle) {
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(ex.parameters(), params, req_info);

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

void service::command_explain_by_text(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    auto& ex = proto_req.explain_by_text();
    auto const& sql = ex.sql();
    if(sql.empty()) {
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "invalid request format - missing sql",
            status::err_invalid_argument
        );
        details::error<sql::response::Explain>(*res, err_info.get(), req_info);
        return;
    }
    // log explain event here to include db_->prepare duration as well as db_->explain
    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::explain);
    req->statement_text(std::make_shared<std::string>(sql));  //NOLINT
    req->status(scheduler::request_detail_status::accepted);
    log_request(*req);

    jogasaki::api::statement_handle statement{};
    std::shared_ptr<error::error_info> err_info{};
    plan::compile_option option{};
    option.explain_by_text_only(true);
    if(auto rc = get_impl(*db_).prepare(sql, statement, err_info, option); rc != jogasaki::status::ok) {
        details::error<sql::response::Explain>(*res, err_info.get(), req_info);
        req->status(scheduler::request_detail_status::finishing);
        log_request(*req, false);
        return;
    }

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    err_info = {};
    auto params = jogasaki::api::create_parameter_set();
    if(auto rc = get_impl(*db_).resolve(statement, maybe_shared_ptr{params.get()}, e, err_info);
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

template<class Request>
std::shared_ptr<impl::prepared_statement> extract_statement(
    Request msg,
    api::database* db,
    std::shared_ptr<error::error_info>& out
) {
    if(! msg.has_prepared_statement_handle()) {
        out = create_error_info(
            error_code::statement_not_found_exception,
            "Invalid request format - missing prepared_statement_handle",
            status::err_invalid_argument
        );
        return {};
    }
    jogasaki::api::statement_handle handle{msg.prepared_statement_handle().handle(), reinterpret_cast<std::uintptr_t>(db)}; //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    auto stmt = get_statement(handle);
    if (stmt == nullptr) {
        auto m = string_builder{} << "prepared statement not found for handle:" << handle.get() << string_builder::to_string;
        out = create_error_info(
            error_code::statement_not_found_exception,
            m,
            status::err_invalid_argument
        );
        return {};
    }
    return stmt;
}

bool extract_sql_and_tx_id(
    sql::request::Request const& req,
    api::database* db,
    std::shared_ptr<std::string>& sql_text,
    std::string& tx_id,
    std::shared_ptr<error::error_info>& err_info,
    request_info const& req_info
) {
    switch (req.request_case()) {
        case sql::request::Request::RequestCase::kExecuteStatement: {
            auto& msg = req.execute_statement();
            sql_text = std::make_shared<std::string>(msg.sql());
            tx_id = extract_transaction(msg, db, err_info, req_info);
            if(err_info) {
                return false;
            }
            break;
        }
        case sql::request::Request::RequestCase::kExecuteQuery: {
            auto& msg = req.execute_query();
            sql_text = std::make_shared<std::string>(msg.sql());
            tx_id = extract_transaction(msg, db, err_info, req_info);
            if(err_info) {
                return false;
            }
            break;
        }
        case sql::request::Request::RequestCase::kExecutePreparedStatement: {
            auto& msg = req.execute_prepared_statement();
            auto stmt = extract_statement(msg, db, err_info);
            if(! stmt) {
                return false;
            }
            sql_text = stmt->body()->sql_text_shared();
            tx_id = extract_transaction(msg, db, err_info, req_info);
            if(err_info) {
                return false;
            }
            break;
        }
        case sql::request::Request::RequestCase::kExecutePreparedQuery: {
            auto& msg = req.execute_prepared_query();
            auto stmt = extract_statement(msg, db, err_info);
            if(! stmt) {
                return false;
            }
            sql_text = stmt->body()->sql_text_shared();
            tx_id = extract_transaction(msg, db, err_info, req_info);
            if(err_info) {
                return false;
            }
            break;
        }
        default: {
            auto msg = string_builder{} << "extracting sql from request payload " << req.request_case()
                                        << " is unsupported" << string_builder::to_string;
            err_info = create_error_info(
                error_code::request_failure_exception,
                msg,
                status::err_unsupported
            );
            return false;
        }
    }
    return true;
}

void service::command_extract_statement_info(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    auto& ex = proto_req.extract_statement_info();
    auto const& payload = ex.payload();
    if(payload.empty()) {
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "invalid request format - missing payload",
            status::err_invalid_argument
        );
        details::error<sql::response::ExtractStatementInfo>(*res, err_info.get(), req_info);
        return;
    }
    sql::request::Request decoded_req{};
    if (! decoded_req.ParseFromArray(payload.data(), static_cast<int>(payload.size()))) {
        auto msg = string_builder{} << "failed to parse payload:" << utils::binary_printer{payload} << string_builder::to_string;
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            msg,
            status::err_invalid_argument
        );
        details::error<sql::response::ExtractStatementInfo>(*res, err_info.get(), req_info);
        return;
    }
    std::shared_ptr<std::string> sql_text{};
    std::shared_ptr<error::error_info> err{};
    std::string tx_id{};
    if(! extract_sql_and_tx_id(decoded_req, db_, sql_text, tx_id, err, req_info)) {
        details::error<sql::response::ExtractStatementInfo>(*res, err.get(), req_info);
        return;
    }
    details::success<sql::response::ExtractStatementInfo>(*res, sql_text, static_cast<std::string_view>(tx_id), req_info);
}

//TODO put global constant file
constexpr static std::size_t max_records_per_file = 10000;

executor::file::time_unit_kind from(::jogasaki::proto::sql::common::TimeUnit kind) {
    using tu = ::jogasaki::proto::sql::common::TimeUnit;
    using k = executor::file::time_unit_kind;
    switch(kind) {
        case tu::NANOSECOND: return k::nanosecond;
        case tu::MICROSECOND: return k::microsecond;
        case tu::MILLISECOND: return k::millisecond;
        // no second on proto, though internally we have one in time_unit_kind to support in the future
        default: return k::unspecified;
    }
    std::abort();
}

void service::command_get_large_object_data(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    auto& gd = proto_req.get_large_object_data();
    if(! gd.has_reference()) {
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "invalid request format - missing reference",
            status::err_invalid_argument
        );
        details::error<sql::response::GetLargeObjectData>(*res, err_info.get(), req_info);
        return;
    }
    auto const& reference = gd.reference();

    std::shared_ptr<error::error_info> err{};
    std::unique_ptr<tateyama::api::server::blob_info> info{};
    if (auto st = datastore::get_lob_data(
            reference.object_id(),
            static_cast<lob::lob_data_provider>(reference.provider()), err,
            info);
        st != status::ok) {
        details::error<sql::response::GetLargeObjectData>(*res, err.get(), req_info);
        return;
    }
    auto* p = info.get();
    if (auto st = res->add_blob(std::move(info)); st != tateyama::status::ok) {
        if (st == tateyama::status::operation_denied) {
            auto err_info = create_error_info(
                error_code::operation_denied,
                "BLOB handling in privileged mode is not allowed",
                status::err_unsupported
            );
            details::error<sql::response::GetLargeObjectData>(*res, err_info.get(), req_info);
            return;
        }
        auto err_info = create_error_info(
            error_code::sql_execution_exception,
            "failed to add blob to response",
            status::err_unknown
        );
        details::error<sql::response::GetLargeObjectData>(*res, err_info.get(), req_info);
        return;
    }
    VLOG_LP(log_trace) << "blob added to response as channel:" << p->channel_name() << " path:" << p->path();

    details::success<sql::response::GetLargeObjectData>(*res, p->channel_name(), req_info);
}

void service::command_get_transaction_status(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    auto& gts = proto_req.get_transaction_status();
    auto tx = validate_transaction_handle<sql::response::GetTransactionStatus>(gts, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto tctx = get_transaction_context(tx);
    if(! tctx) {
        // invalid handle
        auto err_info = create_error_info(
            error_code::transaction_not_found_exception,
            "Transaction handle is invalid.",
            status::err_invalid_argument
        );
        details::error<sql::response::GetTransactionStatus>(*res, err_info.get(), req_info);
        return;
    }
    details::success<sql::response::GetTransactionStatus>(*res, req_info, tctx->state());
}

void service::command_execute_dump(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& ed = proto_req.execute_dump();
    auto tx = validate_transaction_handle<sql::response::ResultOnly>(ed, db_, *res, req_info);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(ed, db_, *res, req_info);
    if(! handle) {
        return;
    }

    auto params = jogasaki::api::create_parameter_set();
    set_params(ed.parameters(), params, req_info);

    executor::io::dump_config opts{};
    opts.max_records_per_file_ = (ed.has_option() && ed.option().max_record_count_per_file() > 0) ?
        ed.option().max_record_count_per_file() : 0;
    opts.keep_files_on_error_ = ed.has_option() && ed.option().fail_behavior() == proto::sql::request::KEEP_FILES;
    opts.time_unit_kind_ =
        ed.has_option() ? from(ed.option().timestamp_unit()) : executor::file::time_unit_kind::unspecified;
    if(ed.has_option()) {
        auto& opt = ed.option();
        if(opt.has_arrow()) {
            opts.file_format_ = executor::io::dump_file_format_kind::arrow;
            auto& arrw = opt.arrow();
            opts.record_batch_size_ = arrw.record_batch_size();
            opts.record_batch_in_bytes_ = arrw.record_batch_in_bytes();
            opts.arrow_use_fixed_size_binary_for_char_ = arrw.character_field_type() ==
                ::jogasaki::proto::sql::request::ArrowCharacterFieldType::FIXED_SIZE_BINARY;
        } else {
            opts.file_format_ = executor::io::dump_file_format_kind::parquet;
            if(opts.max_records_per_file_ == 0) {
                // TODO for parquet, spliting to row groups is not implemented yet,
                // so keep the legacy logic of separating files.
                opts.max_records_per_file_ = max_records_per_file;
            }
        }
    }
    execute_dump(
        res,
        details::query_info{handle.get(), std::shared_ptr{std::move(params)}},
        tx,
        ed.directory(),
        opts,
        req_info
    );
}

void service::command_execute_load(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res,
    request_info const& req_info
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& ed = proto_req.execute_load();
    jogasaki::api::transaction_handle tx{};
    if(ed.has_transaction_handle()) {
        tx = validate_transaction_handle<sql::response::ExecuteResult>(ed, db_, *res, req_info);
        if (!tx) {
            return;
        }
    } else {
        // non-transactional load
    }
    auto handle = validate_statement_handle<sql::response::ExecuteResult>(ed, db_, *res, req_info);
    if(! handle) {
        return;
    }

    auto params = jogasaki::api::create_parameter_set();
    set_params(ed.parameters(), params, req_info);
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
    request_info const& req_info
) {
    auto& dt = proto_req.describe_table();

    auto req = std::make_shared<scheduler::request_detail>(scheduler::request_detail_kind::describe_table);
    req->status(scheduler::request_detail_status::accepted);

    log_request(*req);
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    auto table = db_->find_table(dt.name());
    if(! table || utils::is_prefix(dt.name(), system_identifier_prefix)) {
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
    std::shared_ptr<tateyama::api::server::request> req,  //NOLINT(performance-unnecessary-value-param)
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

std::string version_string(std::size_t major, std::size_t minor) {
    return string_builder{} << "sql-" << major << "." << minor << string_builder::to_string;
}

bool check_message_version(
    sql::request::Request const& proto_req,
    tateyama::api::server::response& res,
    std::size_t reqid
) {
    auto major = proto_req.service_message_version_major();
    auto minor = proto_req.service_message_version_minor();
    if(major == service_message_version_major) {
        return true;
    }
    auto msg = string_builder{} <<
        "inconsistent service message version: see"
        " https://github.com/project-tsurugi/tsurugidb/blob/master/docs/service-message-compatibilities.md"
        " (client: \"" << version_string(major, minor) <<
        "\", server: \"" << version_string(service_message_version_major, service_message_version_minor) << "\")" <<
        string_builder::to_string;
    details::report_error(res, tateyama::proto::diagnostics::Code::INVALID_REQUEST, msg, reqid);
    return false;
}

void show_session_variables(tateyama::api::server::request& req) {
    // for degub purpose, print session variables to server log
    if(VLOG_IS_ON(log_trace)) {
        std::stringstream ss{};
        ss << "session variables ";
        std::string_view plan_recording = "<not set>";
        if(auto v = req.session_variable_set().get(session_variable_sql_plan_recording); std::holds_alternative<bool>(v)) {
            plan_recording = std::get<bool>(v) ? "true" : "false";
        }
        ss << session_variable_sql_plan_recording << ":" << plan_recording;
        VLOG(log_trace) << log_location_prefix << ss.str();
    }
}

bool service::process(
    std::shared_ptr<tateyama::api::server::request> req,  //NOLINT(performance-unnecessary-value-param)
    std::shared_ptr<tateyama::api::server::response> res  //NOLINT(performance-unnecessary-value-param)
) {
    std::size_t reqid = request_id_src_++;
    request_info req_info{reqid, req, res};
    sql::request::Request proto_req{};
    thread_local std::atomic_size_t cnt = 0;
    bool enable_performance_counter = false;
    if (++cnt > 0 && cnt % 1000 == 0) { // measure with performance counter on every 1000 invocations
        enable_performance_counter = true;
        LIKWID_MARKER_START("service");
    }
    show_session_variables(*req);
    if(req->session_id() != 0) {
        // TODO temporary fix : not to send back header if request doesn't add session_id, which means legacy request
        res->session_id(req->session_id());
    }
    {
        trace_scope_name("parse_request");  //NOLINT
        auto s = req->payload();
        if (!proto_req.ParseFromArray(s.data(), static_cast<int>(s.size()))) {
            auto msg = string_builder{} << "parse error with request (rid=" << reqid
                                        << ") body:" << utils::binary_printer{s} << string_builder::to_string;
            details::report_error(*res, tateyama::proto::diagnostics::Code::INVALID_REQUEST, msg, reqid);
            return true;
        }
        VLOG(log_trace) << log_location_prefix
                        << "request received (session_id=" << req->session_id()
                        << ",local_id=" << req->local_id() << ",rid=" << reqid
                        << ",len=" << s.size()
                        << "): " << utils::to_debug_string(proto_req);
    }
    if (! db_->config()->skip_smv_check()) {
        if (! check_message_version(proto_req, *res, reqid)) {
            return true;
        }
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
        case sql::request::Request::RequestCase::kBatch: {
            auto msg = string_builder{} << "batch request is unsupported (rid=" << reqid
                                        << ") body:" << utils::to_debug_string(proto_req) << string_builder::to_string;
            details::report_error(*res, tateyama::proto::diagnostics::Code::UNSUPPORTED_OPERATION, msg, reqid);
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
        case sql::request::Request::RequestCase::kExplainByText: {
            trace_scope_name("cmd-explain_by_text");  //NOLINT
            command_explain_by_text(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kExtractStatementInfo: {
            trace_scope_name("cmd-extract_statement_info");  //NOLINT
            command_extract_statement_info(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kGetLargeObjectData: {
            trace_scope_name("cmd-get_large_object_data");  //NOLINT
            command_get_large_object_data(proto_req, res, req_info);
            break;
        }
        case sql::request::Request::RequestCase::kGetTransactionStatus: {
            trace_scope_name("cmd-get_transaction_status");  //NOLINT
            command_get_transaction_status(proto_req, res, req_info);
            break;
        }
        default:
            auto msg = string_builder{} << "request code is invalid (rid=" << reqid
                                        << ") code:" << proto_req.request_case()
                                        << " body:" << utils::to_debug_string(proto_req) << string_builder::to_string;
            details::report_error(*res, tateyama::proto::diagnostics::Code::INVALID_REQUEST, msg, reqid);
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
    request_info const& req_info
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
            [cbp, this, req_info](
                status s,
                std::shared_ptr<api::error_info> info,  //NOLINT(performance-unnecessary-value-param)
                std::shared_ptr<request_statistics> stats
            ){
                if (s == jogasaki::status::ok) {
                    details::success<sql::response::ExecuteResult>(*cbp->response_, req_info, std::move(stats));
                } else {
                    details::error<sql::response::ExecuteResult>(*cbp->response_, info.get(), req_info);
                }
                if(! callbacks_.erase(cbp->id_)) {
                    throw_exception(std::logic_error{"missing callback"});
                }
            },
            req_info
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

void service::set_params(
    ::google::protobuf::RepeatedPtrField<sql::request::Parameter> const& ps,
    std::unique_ptr<jogasaki::api::parameter_set>& params,
    request_info const& req_info
) {
    for (std::size_t i=0, n=static_cast<std::size_t>(ps.size()); i < n; ++i) {
        auto& p = ps.Get(static_cast<int>(i));
        switch (p.value_case()) {
            case sql::request::Parameter::ValueCase::kBooleanValue:
                params->set_boolean(p.name(), static_cast<std::int8_t>(p.boolean_value() ? 1 : 0));
                break;
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
            case sql::request::Parameter::ValueCase::kOctetValue:
                params->set_octet(p.name(), p.octet_value());
                break;
            case sql::request::Parameter::ValueCase::kDecimalValue:
                params->set_decimal(p.name(), to_triple(p.decimal_value()));
                break;
            case sql::request::Parameter::ValueCase::kDateValue:
                params->set_date(p.name(), field_type_traits<kind::date>::parameter_type{p.date_value()});
                break;
            case sql::request::Parameter::ValueCase::kTimeOfDayValue:
                params->set_time_of_day(
                    p.name(),
                    field_type_traits<kind::time_of_day>::parameter_type{
                        std::chrono::duration<std::uint64_t, std::nano>{p.time_of_day_value()}
                    }
                );
                break;
            case sql::request::Parameter::ValueCase::kTimePointValue: {
                auto& v = p.time_point_value();
                params->set_time_point(p.name(), field_type_traits<kind::time_point>::parameter_type{
                    std::chrono::duration<std::int64_t>{v.offset_seconds()},
                    std::chrono::nanoseconds{v.nano_adjustment()}
                });
                break;
            }
            case sql::request::Parameter::ValueCase::kTimeOfDayWithTimeZoneValue: {
                takatori::datetime::time_of_day tod{
                    std::chrono::duration<std::uint64_t, std::nano>{
                        p.time_of_day_with_time_zone_value().offset_nanoseconds()
                    }
                };
                auto offset_min = p.time_of_day_with_time_zone_value().time_zone_offset();
                params->set_time_of_day(p.name(), field_type_traits<kind::time_of_day>::parameter_type{
                    utils::remove_offset({tod, offset_min})
                });
                break;
            }
            case sql::request::Parameter::ValueCase::kTimePointWithTimeZoneValue: {
                auto& v = p.time_point_with_time_zone_value();
                takatori::datetime::time_point tp{
                    std::chrono::duration<std::int64_t>{v.offset_seconds()},
                    std::chrono::nanoseconds{v.nano_adjustment()}
                };
                auto offset_min = v.time_zone_offset();
                params->set_time_point(p.name(), field_type_traits<kind::time_point>::parameter_type{
                    utils::remove_offset({tp, offset_min})
                });
                break;
            }
            case sql::request::Parameter::ValueCase::kBlob: {
                auto& v = p.blob();
                if (req_info.request_source()->has_blob(v.channel_name())) {
                    auto& info = req_info.request_source()->get_blob(v.channel_name());
                    params->set_blob(p.name(), field_type_traits<kind::blob>::parameter_type{info.path().string(), info.is_temporary()});
                }
                break;
            }
            case sql::request::Parameter::ValueCase::kClob: {
                auto& v = p.clob();
                if (req_info.request_source()->has_blob(v.channel_name())) {
                    auto& info = req_info.request_source()->get_blob(v.channel_name());
                    params->set_clob(p.name(), field_type_traits<kind::clob>::parameter_type{info.path().string(), info.is_temporary()});
                }
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
    request_info const& req_info
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
            abort_transaction(tx, req_info, err_info);
            details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
            return;
        }
        has_result_records = e->meta() != nullptr;
    } else {
        jogasaki::api::statement_handle statement{q.sid(), reinterpret_cast<std::uintptr_t>(db_)}; //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
        if(auto rc = get_impl(*db_).resolve(statement, q.params(), e, err_info); rc != jogasaki::status::ok) {
            details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
            return;
        }
        has_result_records = statement.has_result_records();
    }
    if(! has_result_records) {
        auto msg = "statement has no result records, but called with API expecting result records";
        VLOG(log_error) << log_location_prefix << msg;
        auto err_info =
            create_error_info(error_code::inconsistent_statement_exception, msg, status::err_illegal_operation);
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
        return;
    }

    std::shared_ptr<tateyama::api::server::data_channel> ch{};
    {
        trace_scope_name("acquire_channel");  //NOLINT
        const auto max_write_count = get_write_count(*e);
        if (auto rc = res->acquire_channel(info->name_, ch, max_write_count);
            rc != tateyama::status::ok) {
            auto msg = "creating output channel failed (maybe too many requests)";
            auto err_info =
                create_error_info(error_code::sql_limit_reached_exception, msg, status::err_resource_limit_reached);
            details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
            return;
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
            [cbp, this, req_info](
                status s,
                std::shared_ptr<api::error_info> info,  //NOLINT(performance-unnecessary-value-param)
                std::shared_ptr<request_statistics> stats  //NOLINT(performance-unnecessary-value-param)
            ){
                (void) stats; // no stats for query
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
            req_info
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
    request_info const& req_info,
    bool body_head
) {
    std::string ss{};
    if (!r.SerializeToString(&ss)) {
        throw_exception(std::logic_error{"SerializeToOstream failed"});
    }
    if (body_head) {
        trace_scope_name("body_head");  //NOLINT
        VLOG(log_trace) << log_location_prefix << "respond with body_head ("
                        << "session_id=" << req_info.request_source()->session_id()
                        << ",local_id=" << req_info.request_source()->local_id()
                        << ",rid=" << req_info.id()
                        << ",len=" << ss.size() << "): " << utils::to_debug_string(r);
        res.body_head(ss);
        return;
    }
    {
        trace_scope_name("body");  //NOLINT
        VLOG(log_trace) << log_location_prefix << "respond with body ("
                        << "session_id=" << req_info.request_source()->session_id()
                        << ",local_id=" << req_info.request_source()->local_id()
                        << ",rid=" << req_info.id()
                        << ",len=" << ss.size()
                        << "): " << utils::to_debug_string(r);
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
            case jogasaki::api::field_type_kind::decimal:
                column->set_atom_type(sql::common::AtomType::DECIMAL);
                break;
            case jogasaki::api::field_type_kind::character:
                column->set_atom_type(sql::common::AtomType::CHARACTER);
                break;
            case jogasaki::api::field_type_kind::octet:
                column->set_atom_type(sql::common::AtomType::OCTET);
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
            case jogasaki::api::field_type_kind::time_of_day_with_time_zone:
                column->set_atom_type(sql::common::AtomType::TIME_OF_DAY_WITH_TIME_ZONE);
                break;
            case jogasaki::api::field_type_kind::time_point:
                BOOST_ASSERT(fld.time_point_option() != nullptr);  //NOLINT
                if(fld.time_point_option()->with_offset()) {
                    column->set_atom_type(sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE);
                    break;
                }
                column->set_atom_type(sql::common::AtomType::TIME_POINT);
                break;
            case jogasaki::api::field_type_kind::time_point_with_time_zone:
                column->set_atom_type(sql::common::AtomType::TIME_POINT_WITH_TIME_ZONE);
                break;
            case jogasaki::api::field_type_kind::blob:
                column->set_atom_type(sql::common::AtomType::BLOB);
                break;
            case jogasaki::api::field_type_kind::clob:
                column->set_atom_type(sql::common::AtomType::CLOB);
                break;
            case jogasaki::api::field_type_kind::unknown:
                column->set_atom_type(sql::common::AtomType::UNKNOWN);
                break;
            default:
                LOG(ERROR) << log_location_prefix << "unsupported data type at field (" << i
                           << "): " << metadata->at(i).kind();
                break;
        }
    }
}

void service::execute_dump(
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::query_info const& q,
    jogasaki::api::transaction_handle tx,
    std::string_view directory,
    executor::io::dump_config const& opts,
    request_info const& req_info
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
    jogasaki::api::statement_handle statement{q.sid(), reinterpret_cast<std::uintptr_t>(db_)}; //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
    std::shared_ptr<error::error_info> err_info{};
    if(auto rc = get_impl(*db_).resolve(statement, q.params(), e, err_info); rc != jogasaki::status::ok) {
        details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
        return;
    }

    std::shared_ptr<tateyama::api::server::data_channel> ch{};
    {
        trace_scope_name("acquire_channel");  //NOLINT
        const auto max_write_count = get_write_count(*e);
        if (auto rc = res->acquire_channel(info->name_, ch, max_write_count);
            rc != tateyama::status::ok) {
            auto msg = "creating output channel failed (maybe too many requests)";
            auto err_info =
                create_error_info(error_code::sql_limit_reached_exception, msg, status::err_resource_limit_reached);
            details::error<sql::response::ResultOnly>(*res, err_info.get(), req_info);
            return;
        }
    }
    info->data_channel_ = std::make_shared<jogasaki::api::impl::data_channel>(ch);
    {
        auto m = std::make_shared<meta::record_meta>(
            std::vector<meta::field_type>{
                meta::field_type(std::make_shared<meta::character_field_option>()),
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
            std::move(t),
            std::shared_ptr{std::move(e)},
            info->data_channel_,
            directory,
            [cbp, this, req_info](
                status s,
                std::shared_ptr<error::error_info> info //NOLINT(performance-unnecessary-value-param)
            ) {
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
            opts,
            req_info
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
    request_info const& req_info
) {
    for(auto&& f : files) {
        VLOG(log_info) << log_location_prefix << "load processing file: " << f;
    }
    BOOST_ASSERT(! q.has_sql());  //NOLINT
    jogasaki::api::statement_handle statement{q.sid(), reinterpret_cast<std::uintptr_t>(db_)}; //NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)

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
                [cbp, this, req_info](
                    status s,
                    std::shared_ptr<error::error_info> info  //NOLINT(performance-unnecessary-value-param)
                ) {
                    if (s == jogasaki::status::ok) {
                        auto stats = std::make_shared<request_statistics>();
                        details::success<sql::response::ExecuteResult>(*cbp->response_, req_info, std::move(stats));
                    } else {
                        details::error<sql::response::ExecuteResult>(*cbp->response_, info.get(), req_info);
                    }
                    if (!callbacks_.erase(cbp->id_)) {
                        throw_exception(std::logic_error{"missing callback"});
                    }
                },
                req_info
            ); !rc) {
            // for now execute_async doesn't raise error. But if it happens in future,
            // error response should be sent here.
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
                        auto stats = std::make_shared<request_statistics>();
                        details::success<sql::response::ExecuteResult>(*cbp->response_, req_info, std::move(stats));
                    } else {
                        details::error<sql::response::ExecuteResult>(*cbp->response_, err_info.get(), req_info);
                    }
                    if (!callbacks_.erase(cbp->id_)) {
                        throw_exception(std::logic_error{"missing callback"});
                    }
                }
            ); !rc) {
            // for now execute_async doesn't raise error. But if it happens in future,
            // error response should be sent here.
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

std::size_t service::get_write_count(jogasaki::api::executable_statement const& es) const noexcept {
    const auto& impl_stmt = get_impl(es);
    const auto partitions = impl_stmt.body()->mirrors()->get_partitions();
    if (VLOG_IS_ON(log_debug)) {
        std::stringstream ss{};
        ss << "write_count:" << partitions << " Use calculate_partition";
        VLOG_LP(log_debug) << ss.str();
    }
    return partitions;
}

}  // namespace jogasaki::api::impl
