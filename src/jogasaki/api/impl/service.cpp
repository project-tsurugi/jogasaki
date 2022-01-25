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

#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <tateyama/api/registry.h>
#include <tateyama/api/environment.h>

namespace jogasaki::api::impl {

using takatori::util::fail;
using takatori::util::maybe_shared_ptr;
using namespace tateyama::api::server;
using respose_code = tateyama::api::endpoint::response_code;

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
    ::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    (void)proto_req;
    jogasaki::api::transaction_handle tx{};
    if (auto st = db_->create_transaction(tx); st == jogasaki::status::ok) {
        details::success<::response::Begin>(*res, tx);
    } else {
        details::error<::response::Begin>(*res, st, "error in db_->create_transaction()");
    }
}

void service::command_prepare(
    ::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& pp = proto_req.prepare();
    auto& hvs = pp.host_variables();
    auto& sql = pp.sql();
    if(sql.empty()) {
        VLOG(log_error) << "missing sql";
        details::error<::response::Prepare>(*res, status::err_invalid_argument, "missing sql");
        return;
    }

    std::unordered_map<std::string, jogasaki::api::field_type_kind> variables{};
    for(std::size_t i = 0; i < static_cast<std::size_t>(hvs.variables_size()) ;i++) {
        auto& hv = hvs.variables(i);
        variables.emplace(hv.name(), jogasaki::utils::type_for(hv.type()));
    }
    jogasaki::api::statement_handle statement{};
    if(auto rc = db_->prepare(sql, variables, statement); rc == jogasaki::status::ok) {
        details::success<::response::Prepare>(*res, statement);
    } else {
        details::error<::response::Prepare>(*res, rc, "error in db_->prepare()");
    }
}

template<class T>
jogasaki::api::transaction_handle validate_transaction_handle(
    T msg,
    tateyama::api::server::response& res
) {
    if(! msg.has_transaction_handle()) {
        VLOG(log_error) << "missing transaction_handle";
        details::error<::response::ResultOnly>(res, status::err_invalid_argument, "missing transaction_handle");
        return {};
    }
    jogasaki::api::transaction_handle tx{msg.transaction_handle().handle()};
    if(! tx) {
        details::error<::response::ResultOnly>(res, jogasaki::status::err_invalid_argument, "invalid transaction handle");
        return {};
    }
    return tx;
}

void service::command_execute_statement(
    ::request::Request const& proto_req,
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
        details::error<::response::ResultOnly>(*res, status::err_invalid_argument, "missing sql");
        return;
    }
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->create_executable(sql, e); rc != jogasaki::status::ok) {
        VLOG(log_error) << "error in db_->create_executable()";
        details::error<::response::ResultOnly>(*res, rc, "error in db_->create_executable()");
        return;
    }
    execute_statement(res, std::shared_ptr{std::move(e)}, tx);
}

void service::command_execute_query(
    ::request::Request const& proto_req,
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
        details::error<::response::ResultOnly>(*res, status::err_invalid_argument, "missing sql");
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
    ::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& pq = proto_req.execute_prepared_statement();
    auto tx = validate_transaction_handle(pq, *res);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<::response::ResultOnly>(pq, *res);
    if(! handle) {
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(pq.parameters(), params);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->resolve(handle, std::shared_ptr{std::move(params)}, e); rc != jogasaki::status::ok) {
        VLOG(log_error) << "error in db_->resolve()";
        details::error<::response::ResultOnly>(*res, rc, "error in db_->resolve()");
        return;
    }
    execute_statement(res, std::shared_ptr{std::move(e)}, tx);
}

void service::command_execute_prepared_query(
    ::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    // beware asynchronous call : stack will be released soon after submitting request
    auto& pq = proto_req.execute_prepared_query();
    auto tx = validate_transaction_handle(pq, *res);
    if(! tx) {
        return;
    }
    auto handle = validate_statement_handle<::response::ResultOnly>(pq, *res);
    if(! handle) {
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(pq.parameters(), params);
    execute_query(res, details::query_info{handle.get(), std::shared_ptr{std::move(params)}}, tx);
}
void service::command_commit(
    ::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& cm = proto_req.commit();
    auto tx = validate_transaction_handle(cm, *res);
    if(! tx) {
        return;
    }
    if(auto rc = tx.commit(); rc == jogasaki::status::ok) {
        details::success<::response::ResultOnly>(*res);
    } else {
        VLOG(log_error) << "error in transaction_->commit()";
        details::error<::response::ResultOnly>(*res, rc, "error in transaction_->commit()");
        // currently, commit failure is assumed to abort the transaction anyway.
        // So let's proceed to destroy the transaction.
    }
    if (auto st = db_->destroy_transaction(tx); st != jogasaki::status::ok) {
        fail();
    }
}
void service::command_rollback(
    ::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& rb = proto_req.rollback();
    auto tx = validate_transaction_handle(rb, *res);
    if(! tx) {
        return;
    }
    if(auto rc = tx.abort(); rc == jogasaki::status::ok) {
        details::success<::response::ResultOnly>(*res);
    } else {
        VLOG(log_error) << "error in transaction_->abort()";
        details::error<::response::ResultOnly>(*res, rc, "error in transaction_->abort()");
        // currently, we assume this won't happen or the transaction is aborted anyway.
        // So let's proceed to destroy the transaction.
    }
    if (auto st = db_->destroy_transaction(tx); st != jogasaki::status::ok) {
        fail();
    }
}

void service::command_dispose_prepared_statement(
    ::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& ds = proto_req.dispose_prepared_statement();

    auto handle = validate_statement_handle<::response::ResultOnly>(ds, *res);
    if(! handle) {
        return;
    }
    if(auto st = db_->destroy_statement(handle);
        st == jogasaki::status::ok) {
        details::success<::response::ResultOnly>(*res);
    } else {
        VLOG(log_error) << "error destroying statement";
        details::error<::response::ResultOnly>(*res, st, "error destroying statement");
    }
}
void service::command_disconnect(
    ::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    (void)proto_req;
    details::success<::response::ResultOnly>(*res);
    res->close_session(); //TODO re-visit when the notion of session is finalized
}

void service::command_explain(
    ::request::Request const& proto_req,
    std::shared_ptr<tateyama::api::server::response> const& res
) {
    auto& ex = proto_req.explain();
    auto handle = validate_statement_handle<::response::Explain>(ex, *res);
    if(! handle) {
        return;
    }
    auto params = jogasaki::api::create_parameter_set();
    set_params(ex.parameters(), params);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->resolve(handle, std::shared_ptr{std::move(params)}, e);
        rc != jogasaki::status::ok) {
        VLOG(log_error) << "error in db_->resolve() : " << rc;
        details::error<::response::Explain>(*res, rc, "error in db_->resolve()");
        return;
    }
    std::stringstream ss{};
    if (auto st = db_->explain(*e, ss); st == jogasaki::status::ok) {
        details::success<::response::Explain>(*res, ss.str());
    } else {
        details::error<::response::Explain>(*res, st, "error in db_->explain()");
    }
}

tateyama::status service::operator()(
    std::shared_ptr<tateyama::api::server::request const> req,
    std::shared_ptr<tateyama::api::server::response> res
) {
    ::request::Request proto_req{};
    thread_local std::atomic_size_t cnt = 0;
    bool enable_performance_counter = false;
    if (++cnt > 0 && cnt % 1000 == 0) { // measure with performance counter on every 1000 invocations
        enable_performance_counter = true;
        LIKWID_MARKER_START("service");
    }
    {
        trace_scope_name("parse_request");  //NOLINT
        auto s = req->payload();
        if (!proto_req.ParseFromArray(s.data(), s.size())) {
            VLOG(log_error) << "parse error";
            res->code(response_code::io_error);
            std::string msg{"parse error with request body"};
            VLOG(log_info) << "respond with body (len=" << msg.size() << "):" << std::endl << msg;
            res->body(msg);
            return tateyama::status::ok;
        }
        VLOG(log_info) << "request received (len=" << s.size() << "):" << std::endl << proto_req.Utf8DebugString();
    }

    switch (proto_req.request_case()) {
        case ::request::Request::RequestCase::kBegin: {
            trace_scope_name("cmd-begin");  //NOLINT
            command_begin(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kPrepare: {
            trace_scope_name("cmd-prepare");  //NOLINT
            command_prepare(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kExecuteStatement: {
            trace_scope_name("cmd-execute_statement");  //NOLINT
            command_execute_statement(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kExecuteQuery: {
            trace_scope_name("cmd-execute_query");  //NOLINT
            command_execute_query(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kExecutePreparedStatement: {
            trace_scope_name("cmd-execute_prepared_statement");  //NOLINT
            command_execute_prepared_statement(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kExecutePreparedQuery: {
            trace_scope_name("cmd-execute_prepared_query");  //NOLINT
            command_execute_prepared_query(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kCommit: {
            trace_scope_name("cmd-commit");  //NOLINT
            command_commit(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kRollback: {
            trace_scope_name("cmd-rollback");  //NOLINT
            command_rollback(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kDisposePreparedStatement: {
            trace_scope_name("cmd-dispose_prepared_statement");  //NOLINT
            command_dispose_prepared_statement(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kDisconnect: {
            trace_scope_name("cmd-disconnect");  //NOLINT
            command_disconnect(proto_req, res);
            break;
        }
        case ::request::Request::RequestCase::kExplain: {
            trace_scope_name("cmd-explain");  //NOLINT
            command_explain(proto_req, res);
            break;
        }
        default:
            std::string msg{"invalid request code"};
            VLOG(log_error) << msg;
            res->code(response_code::io_error);
            VLOG(log_info) << "respond with body (len=" << msg.size() << "):" << std::endl << msg;
            res->body(msg);
            break;
    }
    if (enable_performance_counter) {
        LIKWID_MARKER_STOP("service");
    }
    return tateyama::status::ok;
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
                    details::success<::response::ResultOnly>(*cbp->response_);
                } else {
                    details::error<::response::ResultOnly>(*cbp->response_, s, std::string{message});
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

void service::set_params(::request::ParameterSet const& ps, std::unique_ptr<jogasaki::api::parameter_set>& params) {
    for (std::size_t i = 0; i < static_cast<std::size_t>(ps.parameters_size()) ;i++) {
        auto& p = ps.parameters(i);
        switch (p.value_case()) {
            case ::request::ParameterSet::Parameter::ValueCase::kInt4Value:
                params->set_int4(p.name(), p.int4_value());
                break;
            case ::request::ParameterSet::Parameter::ValueCase::kInt8Value:
                params->set_int8(p.name(), p.int8_value());
                break;
            case ::request::ParameterSet::Parameter::ValueCase::kFloat4Value:
                params->set_float4(p.name(), p.float4_value());
                break;
            case ::request::ParameterSet::Parameter::ValueCase::kFloat8Value:
                params->set_float8(p.name(), p.float8_value());
                break;
            case ::request::ParameterSet::Parameter::ValueCase::kCharacterValue:
                params->set_character(p.name(), p.character_value());
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
            details::error<::response::ResultOnly>(*res, rc, "error in db_->create_executable()");
            return;
        }
    } else {
        jogasaki::api::statement_handle statement{q.sid()};
        if(auto rc = db_->resolve(statement, q.params(), e); rc != jogasaki::status::ok) {
            VLOG(log_error) << "error in db_->resolve() : " << rc;
            details::error<::response::ResultOnly>(*res, rc, "error in db_->resolve()");
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
                    details::success<::response::ResultOnly>(*cbp->response_);
                } else {
                    details::error<::response::ResultOnly>(*cbp->response_, s, std::string{message});
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

service::service(jogasaki::api::database& db) :
    db_(std::addressof(db))
{}

std::size_t service::new_resultset_id() const noexcept {
    static std::atomic_size_t resultset_id{};
    return ++resultset_id;
}

tateyama::status service::initialize(tateyama::api::environment& env, void* context) {
    (void)env;
    db_ = reinterpret_cast<jogasaki::api::database*>(context);  //NOLINT
    LIKWID_MARKER_INIT;
    return tateyama::status::ok;
}

tateyama::status service::shutdown() {
    db_ = nullptr;
    LIKWID_MARKER_CLOSE;
    return tateyama::status::ok;
}

void details::reply(tateyama::api::server::response& res, ::response::Response& r, bool body_head) {
    std::stringstream ss{};
    if (!r.SerializeToOstream(&ss)) {
        fail();
    }
    if (body_head) {
        trace_scope_name("body_head");  //NOLINT
        VLOG(log_info) << "respond with body_head (len=" << ss.str().size() << "):" << std::endl << r.Utf8DebugString();
        res.body_head(ss.str());
        return;
    }
    {
        trace_scope_name("body");  //NOLINT
        VLOG(log_info) << "respond with body (len=" << ss.str().size() << "):" << std::endl << r.Utf8DebugString();
        res.body(ss.str());
    }
}

void details::set_metadata(channel_info const& info, schema::RecordMeta& meta) {
    auto* metadata = info.meta_;
    std::size_t n = metadata->field_count();

    for (std::size_t i = 0; i < n; i++) {
        auto column = std::make_unique<::schema::RecordMeta_Column>();
        switch(metadata->at(i).kind()) {
            case jogasaki::api::field_type_kind::int4:
                column->set_type(::common::DataType::INT4);
                column->set_nullable(metadata->nullable(i));
                *meta.add_columns() = *column;
                break;
            case jogasaki::api::field_type_kind::int8:
                column->set_type(::common::DataType::INT8);
                column->set_nullable(metadata->nullable(i));
                *meta.add_columns() = *column;
                break;
            case jogasaki::api::field_type_kind::float4:
                column->set_type(::common::DataType::FLOAT4);
                column->set_nullable(metadata->nullable(i));
                *meta.add_columns() = *column;
                break;
            case jogasaki::api::field_type_kind::float8:
                column->set_type(::common::DataType::FLOAT8);
                column->set_nullable(metadata->nullable(i));
                *meta.add_columns() = *column;
                break;
            case jogasaki::api::field_type_kind::character:
                column->set_type(::common::DataType::CHARACTER);
                column->set_nullable(metadata->nullable(i));
                *meta.add_columns() = *column;
                break;
            default:
                LOG(ERROR) << "unsupported data type at field (" << i << "): " << metadata->at(i).kind();
                fail();
        }
    }
}

}

register_component(server, tateyama::api::server::service, jogasaki, jogasaki::api::impl::service::create);  //NOLINT
