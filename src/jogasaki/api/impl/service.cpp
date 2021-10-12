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

#include <string_view>
#include <memory>

#include <msgpack.hpp>
#include <takatori/util/downcast.h>
#include <takatori/util/fail.h>

#include <jogasaki/status.h>
#include <jogasaki/api/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/utils/proto_field_types.h>

#include <tateyama/api/server/request.h>
#include <tateyama/api/server/response.h>
#include <jogasaki/kvs/database.h>

#include "schema.pb.h"
#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"

namespace jogasaki::api::impl {

using takatori::util::fail;
using namespace tateyama::api::server;
using respose_code = tateyama::api::endpoint::response_code;

namespace details {

class query_info {
public:
    using handle_parameters = std::pair<std::size_t, jogasaki::api::parameter_set*>;
    explicit query_info(std::string_view sql) :
        entity_(std::in_place_type<std::string_view>, sql)
    {}

    explicit query_info(std::size_t sid, jogasaki::api::parameter_set* params) :
        entity_(std::in_place_type<handle_parameters>, std::pair{sid, params})
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

    [[nodiscard]] jogasaki::api::parameter_set* params() const noexcept {
        if (has_sql()) fail();
        return std::get_if<handle_parameters>(std::addressof(entity_))->second;
    }
private:
    std::variant<std::string_view, handle_parameters> entity_{};
};

}

tateyama::status service::operator()(
    std::shared_ptr<tateyama::api::server::request const> req,
    std::shared_ptr<tateyama::api::server::response> res
) {
    ::request::Request proto_req{};
    if (!proto_req.ParseFromString(std::string(req->payload()))) {
        LOG(ERROR) << "parse error" << std::endl;
        res->code(response_code::io_error);
        res->body("parse error with request body");
        return tateyama::status::ok;
    }
    VLOG(1) << "s:" << proto_req.session_handle().handle() << std::endl;

    switch (proto_req.request_case()) {
        case ::request::Request::RequestCase::kBegin: {
            VLOG(1) << "begin" << std::endl;
            jogasaki::api::transaction_handle tx{};
            if (auto st = db_->create_transaction(tx); st == jogasaki::status::ok) {
                details::success<::response::Begin>(*res, tx);
            } else {
                details::error<::response::Begin>(*res, st, "error in db_->create_transaction()");
            }
            break;
        }
        case ::request::Request::RequestCase::kPrepare: {
            VLOG(1) << "prepare" << std::endl;
            auto& pp = proto_req.prepare();
            auto& hvs = pp.host_variables();
            auto& sql = pp.sql();
            if(sql.empty()) LOG(WARNING) << "missing sql";
            VLOG(1) << sql << std::endl;

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
            break;
        }
        case ::request::Request::RequestCase::kExecuteStatement: {
            VLOG(1) << "execute_statement" << std::endl;
            auto& eq = proto_req.execute_statement();
            if(! eq.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            auto& sql = eq.sql();
            if(sql.empty()) LOG(WARNING) << "missing sql";
            jogasaki::api::transaction_handle tx{eq.transaction_handle().handle()};
            VLOG(1) << tx << " " << sql << std::endl;
            if(! tx) {
                details::error<::response::ResultOnly>(*res, jogasaki::status::err_invalid_argument, "invalid transaction handle");
                break;
            }
            std::unique_ptr<jogasaki::api::executable_statement> e{};
            if(auto rc = db_->create_executable(sql, e); rc != jogasaki::status::ok) {
                LOG(ERROR) << "error in db_->create_executable()";
                details::error<::response::ResultOnly>(*res, rc, "error in db_->create_executable()");
                break;
            }
            execute_statement(res, *e, tx);
            break;
        }
        case ::request::Request::RequestCase::kExecuteQuery: {
            VLOG(1) << "execute_query" << std::endl;
            auto& eq = proto_req.execute_query();
            if(! eq.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            auto& sql = eq.sql();
            if(sql.empty()) LOG(WARNING) << "missing sql";
            jogasaki::api::transaction_handle tx{eq.transaction_handle().handle()};
            VLOG(1) << tx << " " << sql << std::endl;
            if(! tx) {
                details::error<::response::ResultOnly>(
                    *res,
                    jogasaki::status::err_invalid_argument,
                    "invalid transaction handle"
                );
                break;
            }

            if (auto rc = execute_query(res, details::query_info{sql}, tx); rc != jogasaki::status::ok) {
                fail();
            }
            break;
        }
        case ::request::Request::RequestCase::kExecutePreparedStatement: {
            VLOG(1) << "execute_prepared_statement" << std::endl;
            auto& pq = proto_req.execute_prepared_statement();
            if(! pq.has_prepared_statement_handle()) LOG(WARNING) << "missing prepared_statement_handle";
            auto& ph = pq.prepared_statement_handle();
            if(! pq.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            auto sid = ph.handle();
            jogasaki::api::transaction_handle tx{pq.transaction_handle().handle()};
            VLOG(1) << tx << " sid:" << sid << std::endl;
            if(! tx) {
                details::error<::response::ResultOnly>(
                    *res,
                    jogasaki::status::err_invalid_argument,
                    "invalid transaction handle"
                );
                break;
            }
            auto params = jogasaki::api::create_parameter_set();
            set_params(pq.parameters(), params);

            jogasaki::api::statement_handle handle{sid};
            std::unique_ptr<jogasaki::api::executable_statement> e{};
            if(auto rc = db_->resolve(handle, *params, e); rc != jogasaki::status::ok) {
                LOG(ERROR) << "error in db_->resolve()";
                details::error<::response::ResultOnly>(*res, rc, "error in db_->resolve()");
                break;
            }
            execute_statement(res, *e, tx);
            break;
        }
        case ::request::Request::RequestCase::kExecutePreparedQuery: {
            VLOG(1) << "execute_prepared_query" << std::endl;
            auto& pq = proto_req.execute_prepared_query();
            if(! pq.has_prepared_statement_handle()) LOG(WARNING) << "missing prepared_statement_handle";
            auto& ph = pq.prepared_statement_handle();
            if(! pq.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            auto sid = ph.handle();
            jogasaki::api::transaction_handle tx{pq.transaction_handle().handle()};
            VLOG(1) << tx << " sid:" << sid << std::endl;
            if(! tx) {
                details::error<::response::ResultOnly>(
                    *res,
                    jogasaki::status::err_invalid_argument,
                    "invalid transaction handle"
                );
                break;
            }
            auto params = jogasaki::api::create_parameter_set();
            set_params(pq.parameters(), params);

            if(auto rc = execute_query(res, details::query_info{sid, params.get()}, tx);
                rc != jogasaki::status::ok) {
                fail();
            }
            break;
        }
        case ::request::Request::RequestCase::kCommit: {
            VLOG(1) << "commit" << std::endl;
            auto& cm = proto_req.commit();
            if(! cm.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            jogasaki::api::transaction_handle tx{cm.transaction_handle().handle()};
            VLOG(1) << tx << std::endl;
            if(! tx) {
                details::error<::response::ResultOnly>(
                    *res,
                    jogasaki::status::err_invalid_argument,
                    "invalid transaction handle"
                );
                break;
            }
            if(auto rc = tx->commit(); rc == jogasaki::status::ok) {
                details::success<::response::ResultOnly>(*res);
            } else {
                details::error<::response::ResultOnly>(*res, rc, "error in transaction_->commit()");
            }
            if (auto st = db_->destroy_transaction(tx); st != jogasaki::status::ok) {
                fail();
            }
            break;
        }
        case ::request::Request::RequestCase::kRollback: {
            VLOG(1) << "rollback" << std::endl;
            auto& rb = proto_req.rollback();
            if(! rb.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            jogasaki::api::transaction_handle tx{rb.transaction_handle().handle()};
            VLOG(1) << tx << std::endl;
            if(! tx) {
                details::error<::response::ResultOnly>(
                    *res,
                    jogasaki::status::err_invalid_argument,
                    "invalid transaction handle"
                );
                break;
            }
            if(auto rc = tx->abort(); rc == jogasaki::status::ok) {
                details::success<::response::ResultOnly>(*res);
            } else {
                details::error<::response::ResultOnly>(*res, rc, "error in transaction_->abort()");
            }
            if (auto st = db_->destroy_transaction(tx); st != jogasaki::status::ok) {
                fail();
            }
            break;
        }
        case ::request::Request::RequestCase::kDisposePreparedStatement: {
            VLOG(1) << "dispose_prepared_statement" << std::endl;
            auto& ds = proto_req.dispose_prepared_statement();
            if(! ds.has_prepared_statement_handle()) LOG(WARNING) << "missing prepared_statement_handle";
            auto& sh = ds.prepared_statement_handle();
            if(auto st = db_->destroy_statement(jogasaki::api::statement_handle{sh.handle()});
                st == jogasaki::status::ok) {
                details::success<::response::ResultOnly>(*res);
            } else {
                details::error<::response::ResultOnly>(*res, st, "error destroying statement");
            }
            break;
        }
        case ::request::Request::RequestCase::kDisconnect: {
            VLOG(1) << "disconnect" << std::endl;
            details::success<::response::ResultOnly>(*res);
            res->close_session(); //TODO re-visit when the notion of session is finalized
            break;
        }
        default:
            LOG(ERROR) << "invalid error case" << std::endl;
            res->code(response_code::io_error);
            res->body("invalid request code");
            break;
    }

    return tateyama::status::ok;
}

void service::execute_statement(
    std::shared_ptr<tateyama::api::server::response>& res,
    jogasaki::api::executable_statement& stmt,
    jogasaki::api::transaction_handle tx
) {
    auto c = std::make_shared<callback_control>(res);
    auto* cbp = c.get();
    callbacks_.emplace(cbp, std::move(c));
    if(auto success = tx->execute_async(
            stmt,
            [cbp, this](status s, std::string_view message){
                if (s == jogasaki::status::ok) {
                    details::success<::response::ResultOnly>(*cbp->response_);
                } else {
                    details::error<::response::ResultOnly>(*cbp->response_, s, std::string{message});
                }
                if(! callbacks_.erase(cbp)) {
                    fail();
                }
            }
        );! success) {
        VLOG(1) << "error in transaction_->execute_async()";
    }
}

//void service::release_writers(
//    tateyama::api::server::response& res,
//    details::channel_info& info
//) {
//    if (info.data_channel_) {
//        res.release_channel(*info.data_channel_);
//        info.data_channel_ = nullptr;
//    }
//}

void service::set_params(::request::ParameterSet const& ps, std::unique_ptr<jogasaki::api::parameter_set>& params)
{
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
                std::cerr << "type undefined" << std::endl;
                std::abort();
                break;
        }
    }
}

jogasaki::status service::execute_query(
    std::shared_ptr<tateyama::api::server::response> res,
    details::query_info const& q,
    jogasaki::api::transaction_handle tx
) {
    if (!tx) {
        fail();
    }
    auto c = std::make_shared<callback_control>(res);
    auto& info = c->channel_info_;
    info = std::make_unique<details::channel_info>();
    info->name_ = std::string("resultset-");
    info->name_ += std::to_string(new_resultset_id());
    std::shared_ptr<tateyama::api::server::data_channel> ch{};
    if(auto rc = res->acquire_channel(info->name_, ch); rc != tateyama::status::ok) {
        fail();
    }
    info->data_channel_ = std::make_shared<jogasaki::api::impl::data_channel>(std::move(ch));

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(q.has_sql()) {
        if(auto rc = db_->create_executable(q.sql(), e); rc != jogasaki::status::ok) {
            LOG(ERROR) << "error in db_->create_executable()";
            return rc;
        }
    } else {
        jogasaki::api::statement_handle statement{q.sid()};
        if(auto rc = db_->resolve(statement, *q.params(), e); rc != jogasaki::status::ok) {
            LOG(ERROR) << "error in db_->resolve()";
            return rc;
        }
    }
    info->meta_ = e->meta();
    auto* cbp = c.get();
    callbacks_.emplace(cbp, c);
    if(auto rc = tx->execute_async(
            *e,
            *info->data_channel_,
            [cbp, this](status s, std::string_view message){
                cbp->response_->release_channel(*cbp->channel_info_->data_channel_->origin());
                if (s == jogasaki::status::ok) {
                    details::success<::response::ResultOnly>(*cbp->response_);
                } else {
                    details::error<::response::ResultOnly>(*cbp->response_, s, std::string{message});
                }
                if(! callbacks_.erase(cbp)) {
                    fail();
                }
            }
        ); ! rc) {
        fail();
    }
    details::send_body_head(*res, info.get());
    return jogasaki::status::ok;
}

service::service(jogasaki::api::database& db) :
    db_(std::addressof(db))
{}

std::size_t service::new_resultset_id() const noexcept {
    static std::atomic_size_t resultset_id{};
    return ++resultset_id;
}

void details::reply(tateyama::api::server::response& res, ::response::Response& r, bool body_head) {
    std::stringstream ss{};
    if (!r.SerializeToOstream(&ss)) {
        std::abort();
    }
    if (body_head) {
        res.body_head(ss.str());
        return;
    }
    res.body(ss.str());
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
                std::cout << __LINE__ << ":" << i << std::endl;
                std::cerr << "unsupported data type: " << metadata->at(i).kind() << std::endl;
                break;
        }
    }
}

}