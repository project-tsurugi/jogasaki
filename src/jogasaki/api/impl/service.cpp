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
#include <glog/logging.h>
#include <takatori/util/downcast.h>
#include <takatori/util/maybe_shared_ptr.h>
#include <takatori/util/fail.h>

#include <jogasaki/status.h>
#include <jogasaki/common.h>
#include <jogasaki/logging.h>
#include <jogasaki/api/database.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/utils/proto_field_types.h>
#include <jogasaki/api/impl/record_meta.h>
#include <jogasaki/api/impl/transaction.h>
#include <jogasaki/meta/external_record_meta.h>
#include <jogasaki/executor/io/record_channel_adapter.h>

#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/api/environment.h>

namespace jogasaki::api::impl {

using takatori::util::fail;
using takatori::util::maybe_shared_ptr;
using namespace tateyama::api::server;

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
        if (! has_sql()) fail();
        return *std::get_if<std::string_view>(std::addressof(entity_));
    }

    [[nodiscard]] std::size_t sid() const noexcept {
        if (has_sql()) fail();
        return std::get_if<handle_parameters>(std::addressof(entity_))->first;
    }

    [[nodiscard]] maybe_shared_ptr<jogasaki::api::parameter_set const> const& params() const noexcept {
        if (has_sql()) fail();
        return std::get_if<handle_parameters>(std::addressof(entity_))->second;
    }
private:
    std::variant<std::string_view, handle_parameters> entity_{};
};

}

void service::command_begin(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    std::vector<std::string> storages{};
    bool readonly = false;
    bool is_long = false;
    auto& bg = proto_req.begin();
    if(bg.has_option()) {
        auto& op = bg.option();
        if(op.type() == sql::request::TransactionType::READ_ONLY) {
            readonly = true;
        }
        if(op.type() == sql::request::TransactionType::LONG) {
            is_long = true;
            storages.reserve(op.write_preserves().size());
            for(auto&& x : op.write_preserves()) {
                storages.emplace_back(x.table_name());
            }
        }
    }
    transaction_option opts{ readonly, is_long, std::move(storages) };
    jogasaki::api::transaction_handle tx{};
    if (auto st = db_->create_transaction(tx, opts); st == jogasaki::status::ok) {
        details::success<sql::response::Begin>(*res, tx);
    } else {
        details::error<sql::response::Begin>(*res, st, "error in db_->create_transaction()");
    }
}

void service::command_prepare(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& pp = proto_req.prepare();
    auto& phs = pp.placeholders();
    auto& sql = pp.sql();
    if(sql.empty()) {
        VLOG(log_error) << "missing sql";
        details::error<sql::response::Prepare>(*res, status::err_invalid_argument, "missing sql");
        return;
    }

    std::unordered_map<std::string, jogasaki::api::field_type_kind> variables{};
    for(std::size_t i=0, n=static_cast<std::size_t>(phs.size()); i < n ; ++i) {
        auto& ph = phs.Get(i);
        variables.emplace(ph.name(), jogasaki::utils::type_for(ph.type()));
    }
    jogasaki::api::statement_handle statement{};
    if(auto rc = db_->prepare(sql, variables, statement); rc == jogasaki::status::ok) {
        details::success<sql::response::Prepare>(*res, statement);
    } else {
        details::error<sql::response::Prepare>(*res, rc, "error in db_->prepare()");
    }
}

template<class T>
jogasaki::api::transaction_handle validate_transaction_handle(
    T msg,
    tateyama::api::server::response& res
) {
    if(! msg.has_transaction_handle()) {
        VLOG(log_error) << "missing transaction_handle";
        details::error<sql::response::ResultOnly>(res, status::err_invalid_argument, "missing transaction_handle");
        return {};
    }
    jogasaki::api::transaction_handle tx{msg.transaction_handle().handle()};
    if(! tx) {
        details::error<sql::response::ResultOnly>(res, jogasaki::status::err_invalid_argument, "invalid transaction handle");
        return {};
    }
    return tx;
}

void service::command_execute_statement(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& eq = proto_req.execute_statement();
    auto tx = validate_transaction_handle(eq, *res);
    if(! tx) {
        return;
    }
    auto& sql = eq.sql();
    if(sql.empty()) {
        VLOG(log_error) << "missing sql";
        details::error<sql::response::ResultOnly>(*res, status::err_invalid_argument, "missing sql");
        return;
    }
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->create_executable(sql, e); rc != jogasaki::status::ok) {
        VLOG(log_error) << "error in db_->create_executable()";
        details::error<sql::response::ResultOnly>(*res, rc, "error in db_->create_executable()");
        return;
    }
    execute_statement(res, std::shared_ptr{std::move(e)}, tx);
}

void service::command_execute_query(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& eq = proto_req.execute_query();
    auto tx = validate_transaction_handle(eq, *res);
    if(! tx) {
        return;
    }
    auto& sql = eq.sql();
    if(sql.empty()) {
        VLOG(log_error) << "missing sql";
        details::error<sql::response::ResultOnly>(*res, status::err_invalid_argument, "missing sql");
        return;
    }
    execute_query(res, details::query_info{sql}, tx);
}

template<class Response, class Request>
jogasaki::api::statement_handle validate_statement_handle(
    Request msg,
    tateyama::api::server::response& res
) {
    if(! msg.has_prepared_statement_handle()) {
        VLOG(log_error) << "missing prepared_statement_handle";
        details::error<Response>(res, status::err_invalid_argument, "missing prepared_statement_handle");
        return {};
    }
    jogasaki::api::statement_handle handle{msg.prepared_statement_handle().handle()};
    if (! handle) {
        details::error<Response>(
            res,
            jogasaki::status::err_invalid_argument,
            "invalid statement handle"
        );
        return {};
    }
    return handle;
}

void service::command_execute_prepared_statement(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& pq = proto_req.execute_prepared_statement();
    auto tx = validate_transaction_handle(pq, *res);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(pq, *res);
    if(! handle) {
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(pq.parameters(), params);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->resolve(handle, std::shared_ptr{std::move(params)}, e); rc != jogasaki::status::ok) {
        VLOG(log_error) << "error in db_->resolve()";
        details::error<sql::response::ResultOnly>(*res, rc, "error in db_->resolve()");
        return;
    }
    execute_statement(res, std::shared_ptr{std::move(e)}, tx);
}

void service::command_execute_prepared_query(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& pq = proto_req.execute_prepared_query();
    auto tx = validate_transaction_handle(pq, *res);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(pq, *res);
    if(! handle) {
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(pq.parameters(), params);
    execute_query(res, details::query_info{handle.get(), std::shared_ptr{std::move(params)}}, tx);
}

void service::command_commit(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& cm = proto_req.commit();
    auto tx = validate_transaction_handle(cm, *res);
    if(! tx) {
        return;
    }
    if(auto rc = tx.commit(); rc == jogasaki::status::ok) {
        details::success<sql::response::ResultOnly>(*res);
    } else {
        VLOG(log_error) << "error in transaction_->commit()";
        details::error<sql::response::ResultOnly>(*res, rc, "error in transaction_->commit()");
        // currently, commit failure is assumed to abort the transaction anyway.
        // So let's proceed to destroy the transaction.
    }
    if (auto st = db_->destroy_transaction(tx); st != jogasaki::status::ok) {
        fail();
    }
}
void service::command_rollback(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& rb = proto_req.rollback();
    auto tx = validate_transaction_handle(rb, *res);
    if(! tx) {
        return;
    }
    if(auto rc = tx.abort(); rc == jogasaki::status::ok) {
        details::success<sql::response::ResultOnly>(*res);
    } else {
        VLOG(log_error) << "error in transaction_->abort()";
        details::error<sql::response::ResultOnly>(*res, rc, "error in transaction_->abort()");
        // currently, we assume this won't happen or the transaction is aborted anyway.
        // So let's proceed to destroy the transaction.
    }
    if (auto st = db_->destroy_transaction(tx); st != jogasaki::status::ok) {
        fail();
    }
}

void service::command_dispose_prepared_statement(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& ds = proto_req.dispose_prepared_statement();

    auto handle = validate_statement_handle<sql::response::ResultOnly>(ds, *res);
    if(! handle) {
        return;
    }
    if(auto st = db_->destroy_statement(handle);
        st == jogasaki::status::ok) {
        details::success<sql::response::ResultOnly>(*res);
    } else {
        VLOG(log_error) << "error destroying statement";
        details::error<sql::response::ResultOnly>(*res, st, "error destroying statement");
    }
}
void service::command_disconnect(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    (void)proto_req;
    details::success<sql::response::ResultOnly>(*res);
    res->close_session(); //TODO re-visit when the notion of session is finalized
}

void service::command_explain(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& ex = proto_req.explain();
    auto handle = validate_statement_handle<sql::response::Explain>(ex, *res);
    if(! handle) {
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(ex.parameters(), params);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->resolve(handle, std::shared_ptr{std::move(params)}, e);
        rc != jogasaki::status::ok) {
        VLOG(log_error) << "error in db_->resolve() : " << rc;
        details::error<sql::response::Explain>(*res, rc, "error in db_->resolve()");
        return;
    }
    std::stringstream ss{};
    if (auto st = db_->explain(*e, ss); st == jogasaki::status::ok) {
        details::success<sql::response::Explain>(*res, ss.str());
    } else {
        details::error<sql::response::Explain>(*res, st, "error in db_->explain()");
    }
}

//TODO put global constant file
constexpr static std::size_t max_records_per_file = 10000;

void service::command_execute_dump(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& ed = proto_req.execute_dump();
    auto tx = validate_transaction_handle(ed, *res);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(ed, *res);
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
    execute_dump(res, details::query_info{handle.get(), std::shared_ptr{std::move(params)}}, tx, ed.directory(), opts);
}

void service::command_execute_load(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& ed = proto_req.execute_load();
    auto tx = validate_transaction_handle(ed, *res);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<sql::response::ResultOnly>(ed, *res);
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
    execute_load(res, details::query_info{handle.get(), std::shared_ptr{std::move(params)}}, tx, files);
}

void service::command_describe_table(
    sql::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& dt = proto_req.describe_table();

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    auto table = db_->find_table(dt.name());
    if(! table) {
        VLOG(log_error) << "table noe found : " << dt.name();
        details::error<sql::response::DescribeTable>(*res, status::err_not_found, "table not found");
        return;
    }
    details::success<sql::response::DescribeTable>(*res, table.get());
}

bool service::operator()(
    std::shared_ptr<tateyama::api::server::request const> req,  //NOLINT(performance-unnecessary-value-param)
    std::shared_ptr<tateyama::api::server::response> res  //NOLINT(performance-unnecessary-value-param)
) {
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
        if (!proto_req.ParseFromArray(s.data(), s.size())) {
            VLOG(log_error) << "parse error";
            res->code(response_code::io_error);
            std::string msg{"parse error with request body"};
            VLOG(log_trace) << "respond with body (len=" << msg.size() << "):" << std::endl << msg;
            res->body(msg);
            return true;
        }
        VLOG(log_trace) << "request received (len=" << s.size() << "):" << std::endl << proto_req.Utf8DebugString();
    }

    switch (proto_req.request_case()) {
        case sql::request::Request::RequestCase::kBegin: {
            trace_scope_name("cmd-begin");  //NOLINT
            command_begin(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kPrepare: {
            trace_scope_name("cmd-prepare");  //NOLINT
            command_prepare(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kExecuteStatement: {
            trace_scope_name("cmd-execute_statement");  //NOLINT
            command_execute_statement(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kExecuteQuery: {
            trace_scope_name("cmd-execute_query");  //NOLINT
            command_execute_query(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kExecutePreparedStatement: {
            trace_scope_name("cmd-execute_prepared_statement");  //NOLINT
            command_execute_prepared_statement(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kExecutePreparedQuery: {
            trace_scope_name("cmd-execute_prepared_query");  //NOLINT
            command_execute_prepared_query(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kCommit: {
            trace_scope_name("cmd-commit");  //NOLINT
            command_commit(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kRollback: {
            trace_scope_name("cmd-rollback");  //NOLINT
            command_rollback(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kDisposePreparedStatement: {
            trace_scope_name("cmd-dispose_prepared_statement");  //NOLINT
            command_dispose_prepared_statement(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kDisconnect: {
            trace_scope_name("cmd-disconnect");  //NOLINT
            command_disconnect(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kExplain: {
            trace_scope_name("cmd-explain");  //NOLINT
            command_explain(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kExecuteDump: {
            trace_scope_name("cmd-dump");  //NOLINT
            command_execute_dump(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kExecuteLoad: {
            trace_scope_name("cmd-load");  //NOLINT
            command_execute_load(proto_req, res);
            break;
        }
        case sql::request::Request::RequestCase::kDescribeTable: {
            trace_scope_name("cmd-describe_table");  //NOLINT
            command_describe_table(proto_req, res);
            break;
        }
        default:
            std::string msg{"invalid request code: "};
            VLOG(log_error) << msg << proto_req.request_case();
            res->code(response_code::io_error);
            VLOG(log_trace) << "respond with body (len=" << msg.size() << "):" << std::endl << msg;
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
    jogasaki::api::transaction_handle tx
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto c = std::make_shared<callback_control>(res);
    auto* cbp = c.get();
    auto cid = c->id_;
    if(! callbacks_.emplace(cid, std::move(c))) {
        fail();
    }
    if(auto success = tx.execute_async(
            std::move(stmt),
            [cbp, this](status s, std::string_view message){
                if (s == jogasaki::status::ok) {
                    details::success<sql::response::ResultOnly>(*cbp->response_);
                } else {
                    details::error<sql::response::ResultOnly>(*cbp->response_, s, std::string{message});
                }
                if(! callbacks_.erase(cbp->id_)) {
                    fail();
                }
            }
        );! success) {
        // normally this should not happen
        fail();
    }
}

void service::set_params(::google::protobuf::RepeatedPtrField<sql::request::Parameter> const& ps, std::unique_ptr<jogasaki::api::parameter_set>& params) {
    for (std::size_t i=0, n=static_cast<std::size_t>(ps.size()); i < n; ++i) {
        auto& p = ps.Get(i);
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
    jogasaki::api::transaction_handle tx
) {
    // beware asynchronous call : stack will be released soon after submitting request
    BOOST_ASSERT(tx);  //NOLINT
    auto c = std::make_shared<callback_control>(res);
    auto& info = c->channel_info_;
    info = std::make_unique<details::channel_info>();
    info->name_ = std::string("resultset-");
    info->name_ += std::to_string(new_resultset_id());
    std::shared_ptr<tateyama::api::server::data_channel> ch{};
    {
        trace_scope_name("acquire_channel");  //NOLINT
        if(auto rc = res->acquire_channel(info->name_, ch); rc != tateyama::status::ok) {
            fail();
        }
    }
    info->data_channel_ = std::make_shared<jogasaki::api::impl::data_channel>(std::move(ch));

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(q.has_sql()) {
        if(auto rc = db_->create_executable(q.sql(), e); rc != jogasaki::status::ok) {
            VLOG(log_error) << "error in db_->create_executable() : " << rc;
            details::error<sql::response::ResultOnly>(*res, rc, "error in db_->create_executable()");
            return;
        }
    } else {
        jogasaki::api::statement_handle statement{q.sid()};
        if(auto rc = db_->resolve(statement, q.params(), e); rc != jogasaki::status::ok) {
            VLOG(log_error) << "error in db_->resolve() : " << rc;
            details::error<sql::response::ResultOnly>(*res, rc, "error in db_->resolve()");
            return;
        }
    }
    info->meta_ = e->meta();
    details::send_body_head(*res, *info);
    auto* cbp = c.get();
    auto cid = c->id_;
    callbacks_.emplace(cid, std::move(c));
    if(auto rc = tx.execute_async(
            std::shared_ptr{std::move(e)},
            info->data_channel_,
            [cbp, this](status s, std::string_view message){
                {
                    trace_scope_name("release_channel");  //NOLINT
                    cbp->response_->release_channel(*cbp->channel_info_->data_channel_->origin());
                }
                if (s == jogasaki::status::ok) {
                    details::success<sql::response::ResultOnly>(*cbp->response_);
                } else {
                    details::error<sql::response::ResultOnly>(*cbp->response_, s, std::string{message});
                }
                if(! callbacks_.erase(cbp->id_)) {
                    fail();
                }
            }
        ); ! rc) {
        // for now execute_async doesn't raise error. But if it happens in future, error response should be sent here.
        fail();
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

void details::reply(tateyama::api::server::response& res, sql::response::Response& r, bool body_head) {
    std::stringstream ss{};
    if (!r.SerializeToOstream(&ss)) {
        fail();
    }
    if (body_head) {
        trace_scope_name("body_head");  //NOLINT
        VLOG(log_trace) << "respond with body_head (len=" << ss.str().size() << "):" << std::endl << r.Utf8DebugString();
        res.body_head(ss.str());
        return;
    }
    {
        trace_scope_name("body");  //NOLINT
        VLOG(log_trace) << "respond with body (len=" << ss.str().size() << "):" << std::endl << r.Utf8DebugString();
        res.body(ss.str());
    }
}

void details::set_metadata(channel_info const& info, sql::response::ResultSetMetadata& meta) {
    auto* metadata = info.meta_;
    std::size_t n = metadata->field_count();

    for (std::size_t i = 0; i < n; i++) {
        auto column = meta.add_columns();
        if(auto name = metadata->field_name(i); name.has_value()) {
            column->set_name(std::string{*name});
        }
        switch(metadata->at(i).kind()) {
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
            default:
                LOG(ERROR) << "unsupported data type at field (" << i << "): " << metadata->at(i).kind();
                fail();
        }
    }
}

void service::execute_dump(
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::query_info const& q,
    jogasaki::api::transaction_handle tx,
    std::string_view directory,
    dump_option const& opts
) {
    // beware asynchronous call : stack will be released soon after submitting request
    BOOST_ASSERT(tx);  //NOLINT
    auto c = std::make_shared<callback_control>(res);
    auto& info = c->channel_info_;
    info = std::make_unique<details::channel_info>();
    info->name_ = std::string("resultset-");
    info->name_ += std::to_string(new_resultset_id());
    std::shared_ptr<tateyama::api::server::data_channel> ch{};
    {
        trace_scope_name("acquire_channel");  //NOLINT
        if(auto rc = res->acquire_channel(info->name_, ch); rc != tateyama::status::ok) {
            fail();
        }
    }
    info->data_channel_ = std::make_shared<jogasaki::api::impl::data_channel>(ch);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    BOOST_ASSERT(! q.has_sql());  //NOLINT
    jogasaki::api::statement_handle statement{q.sid()};
    if(auto rc = db_->resolve(statement, q.params(), e); rc != jogasaki::status::ok) {
        VLOG(log_error) << "error in db_->resolve() : " << rc;
        details::error<sql::response::ResultOnly>(*res, rc, "error in db_->resolve()");
        return;
    }
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
        details::send_body_head(*res, *info);
    }

    auto* cbp = c.get();
    auto cid = c->id_;
    callbacks_.emplace(cid, std::move(c));

    if(auto rc = reinterpret_cast<api::impl::transaction*>(tx.get())->execute_dump(  //NOLINT
            std::shared_ptr{std::move(e)},
            info->data_channel_,
            directory,
            [cbp, this](status s, std::string_view message){
                {
                    trace_scope_name("release_channel");  //NOLINT
                    cbp->response_->release_channel(*cbp->channel_info_->data_channel_->origin());
                }
                if (s == jogasaki::status::ok) {
                    details::success<sql::response::ResultOnly>(*cbp->response_);
                } else {
                    details::error<sql::response::ResultOnly>(*cbp->response_, s, std::string{message});
                }
                if(! callbacks_.erase(cbp->id_)) {
                    fail();
                }
            },
            opts.max_records_per_file_ == 0 ? max_records_per_file : opts.max_records_per_file_,
            opts.keep_files_on_error_
    ); ! rc) {
        // for now execute_async doesn't raise error. But if it happens in future, error response should be sent here.
        fail();
    }
}

void service::execute_load(
    std::shared_ptr<tateyama::api::server::response> const& res,
    details::query_info const& q,
    jogasaki::api::transaction_handle tx,
    std::vector<std::string> const& files
) {
    for(auto&& f : files) {
        LOG(INFO) << "load processing file: " << f;
    }
    BOOST_ASSERT(! q.has_sql());  //NOLINT
    jogasaki::api::statement_handle statement{q.sid()};

    auto c = std::make_shared<callback_control>(res);
    auto* cbp = c.get();
    auto cid = c->id_;
    if(! callbacks_.emplace(cid, std::move(c))) {
        fail();
    }
    if(auto rc = reinterpret_cast<api::impl::transaction*>(tx.get())->execute_load(  //NOLINT
            statement,
            q.params(),
            files,
            [cbp, this](status s, std::string_view message){
                if (s == jogasaki::status::ok) {
                    details::success<sql::response::ResultOnly>(*cbp->response_);
                } else {
                    details::error<sql::response::ResultOnly>(*cbp->response_, s, std::string{message});
                }
                if(! callbacks_.erase(cbp->id_)) {
                    fail();
                }
            }
        ); ! rc) {
        // for now execute_async doesn't raise error. But if it happens in future, error response should be sent here.
        fail();
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
