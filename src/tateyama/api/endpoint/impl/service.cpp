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
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/statement_handle.h>
#include <jogasaki/api/impl/executable_statement.h>
#include <jogasaki/utils/proto_field_types.h>

#include <tateyama/api/endpoint/request.h>
#include <tateyama/api/endpoint/response.h>
#include <jogasaki/kvs/database.h>

#include "schema.pb.h"
#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"

namespace tateyama::api::endpoint::impl {

using takatori::util::fail;

tateyama::status service::operator()(
    std::shared_ptr<tateyama::api::endpoint::request const> req,
    std::shared_ptr<tateyama::api::endpoint::response> res
) {
    ::request::Request proto_req{};
    if (!proto_req.ParseFromString(std::string(req->payload()))) {
        LOG(ERROR) << "parse error" << std::endl;
        res->code(response_code::application_error);
        res->message("parse error with request body");
        res->complete();
        return tateyama::status::ok;
    }
    VLOG(1) << "s:" << proto_req.session_handle().handle() << std::endl;

    switch (proto_req.request_case()) {
        case ::request::Request::RequestCase::kBegin: {
            VLOG(1) << "begin" << std::endl;
            jogasaki::api::transaction_handle tx{};
            if (auto st = db_->create_transaction(tx); st == jogasaki::status::ok) {
                ::common::Transaction t{};
                ::response::Begin b{};
                ::response::Response r{};

                t.set_handle(static_cast<std::size_t>(tx));
                b.set_allocated_transaction_handle(&t);
                r.set_allocated_begin(&b);
                reply(*res, r);
                r.release_begin();
                b.release_transaction_handle();
            } else {
                error<::response::Begin>(*res, "error in db_->create_transaction()");
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
                ::common::PreparedStatement ps{};
                ::response::Prepare p{};
                ::response::Response r{};

                ps.set_handle(static_cast<std::size_t>(statement));
                p.set_allocated_prepared_statement_handle(&ps);
                r.set_allocated_prepare(&p);
                reply(*res, r);
                r.release_prepare();
                p.release_prepared_statement_handle();
            } else {
                error<::response::Prepare>(*res, "error in db_->prepare()");
            }
            break;
        }
        case ::request::Request::RequestCase::kExecuteStatement: {
            VLOG(1) << "execute_statement" << std::endl;
            auto& eq = proto_req.execute_statement();
            if(! eq.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            auto& sql = eq.sql();
            if(sql.empty()) LOG(WARNING) << "missing sql";
            VLOG(1) << "tx:" << eq.transaction_handle().handle() << sql << std::endl;
            jogasaki::api::transaction_handle tx{eq.transaction_handle().handle()};
            if(! tx) {
                error<::response::ResultOnly>(*res, "invalid transaction handle");
                break;
            }
            if (auto err = execute_statement(sql, tx); err == nullptr) {
                ::response::Success s{};
                ::response::ResultOnly ro{};
                ::response::Response r{};
                ro.set_allocated_success(&s);
                r.set_allocated_result_only(&ro);
                reply(*res, r);
                r.release_result_only();
                ro.release_success();
            } else {
                error<::response::ResultOnly>(*res, err);
            }
            break;
        }
        case ::request::Request::RequestCase::kExecuteQuery: {
            VLOG(1) << "execute_query" << std::endl;
            auto& eq = proto_req.execute_query();
            if(! eq.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            auto& sql = eq.sql();
            if(sql.empty()) LOG(WARNING) << "missing sql";
            VLOG(1) << "tx:" << eq.transaction_handle().handle() << sql << std::endl;
            jogasaki::api::transaction_handle tx{eq.transaction_handle().handle()};
            if(! tx) {
                error<::response::ResultOnly>(*res, "invalid transaction handle");
                break;
            }

            if (auto err = execute_query(*res, sql, ++resultset_id_, tx); err == nullptr) {
                ::schema::RecordMeta meta{};
                ::response::ResultSetInfo i{};
                ::response::ExecuteQuery e{};
                ::response::Response r{};

                set_metadata(resultset_id_, meta);
                i.set_name(cursors_.at(resultset_id_).wire_name_);
                i.set_allocated_record_meta(&meta);
                e.set_allocated_result_set_info(&i);
                r.set_allocated_execute_query(&e);
                reply(*res, r);
                r.release_execute_query();
                e.release_result_set_info();
                i.release_record_meta();
                next(resultset_id_);
                release_writers(*res, cursors_.at(resultset_id_));
            } else {
                error<::response::ExecuteQuery>(*res, err);
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
            VLOG(1) << "tx:" << pq.transaction_handle().handle() << "sid:" << sid << std::endl;
            jogasaki::api::transaction_handle tx{pq.transaction_handle().handle()};
            if(! tx) {
                error<::response::ResultOnly>(*res, "invalid transaction handle");
                break;
            }

            auto params = jogasaki::api::create_parameter_set();
            set_params(pq.parameters(), params);
            if (auto err = execute_prepared_statement(sid, *params, tx); err == nullptr) {
                ::response::Success s{};
                ::response::ResultOnly ro{};
                ::response::Response r{};

                ro.set_allocated_success(&s);
                r.set_allocated_result_only(&ro);
                reply(*res, r);
                r.release_result_only();
                ro.release_success();
            } else {
                error<::response::ResultOnly>(*res, err);
            }
            break;
        }
        case ::request::Request::RequestCase::kExecutePreparedQuery: {
            VLOG(1) << "execute_prepared_query" << std::endl;
            auto& pq = proto_req.execute_prepared_query();
            if(! pq.has_prepared_statement_handle()) LOG(WARNING) << "missing prepared_statement_handle";
            auto& ph = pq.prepared_statement_handle();
            if(! pq.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            auto sid = ph.handle();
            VLOG(1) << "tx:" << pq.transaction_handle().handle() << "sid:" << sid << std::endl;
            jogasaki::api::transaction_handle tx{pq.transaction_handle().handle()};
            if(! tx) {
                error<::response::ResultOnly>(*res, "invalid transaction handle");
                break;
            }
            auto params = jogasaki::api::create_parameter_set();
            set_params(pq.parameters(), params);

            if(auto err = execute_prepared_query(*res, sid, *params, ++resultset_id_, tx); err == nullptr) {
                ::schema::RecordMeta meta{};
                ::response::ResultSetInfo i{};
                ::response::ExecuteQuery e{};
                ::response::Response r{};

                set_metadata(resultset_id_, meta);
                i.set_name(cursors_.at(resultset_id_).wire_name_);
                i.set_allocated_record_meta(&meta);
                e.set_allocated_result_set_info(&i);
                r.set_allocated_execute_query(&e);
                reply(*res, r);
                r.release_execute_query();
                e.release_result_set_info();
                i.release_record_meta();

                next(resultset_id_);
                release_writers(*res, cursors_.at(resultset_id_));
            } else {
                error<::response::ExecuteQuery>(*res, err);
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
                error<::response::ResultOnly>(*res, "invalid transaction handle");
                break;
            }
            if(auto rc = tx->commit(); rc == jogasaki::status::ok) {
                ::response::Success s{};
                ::response::ResultOnly ro{};
                ::response::Response r{};

                ro.set_allocated_success(&s);
                r.set_allocated_result_only(&ro);
                reply(*res, r);
                r.release_result_only();
                ro.release_success();
                if (auto st = db_->destroy_transaction(tx); st != jogasaki::status::ok) {
                    fail();
                }
            } else {
                error<::response::ResultOnly>(*res, "error in transaction_->commit()");
            }
            break;
        }
        case ::request::Request::RequestCase::kRollback: {
            VLOG(1) << "rollback" << std::endl;
            auto& rb = proto_req.rollback();
            if(! rb.has_transaction_handle()) LOG(WARNING) << "missing transaction_handle";
            auto& th = rb.transaction_handle();
            VLOG(1) << "tx:" << th.handle() << std::endl;
            jogasaki::api::transaction_handle tx{rb.transaction_handle().handle()};
            if(! tx) {
                error<::response::ResultOnly>(*res, "invalid transaction handle");
                break;
            }
            if(auto rc = tx->abort(); rc == jogasaki::status::ok) {
                ::response::Success s{};
                ::response::ResultOnly ro{};
                ::response::Response r{};

                ro.set_allocated_success(&s);
                r.set_allocated_result_only(&ro);
                reply(*res, r);
                r.release_result_only();
                ro.release_success();

                if (auto st = db_->destroy_transaction(tx); st != jogasaki::status::ok) {
                    fail();
                }
            } else {
                error<::response::ResultOnly>(*res, "error in transaction_->abort()");
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
                ::response::Success s{};
                ::response::ResultOnly ro{};
                ::response::Response r{};

                ro.set_allocated_success(&s);
                r.set_allocated_result_only(&ro);
                reply(*res, r);
                r.release_result_only();
                ro.release_success();
            } else {
                error<::response::ResultOnly>(*res, "error destroying statement");
            }
            break;
        }
        case ::request::Request::RequestCase::kDisconnect: {
            VLOG(1) << "disconnect" << std::endl;
            ::response::Success s{};
            ::response::ResultOnly ro{};
            ::response::Response r{};

            ro.set_allocated_success(&s);
            r.set_allocated_result_only(&ro);
            reply(*res, r);
            r.release_result_only();
            ro.release_success();
            break;
        }
        default:
            LOG(ERROR) << "invalid error case" << std::endl;
            res->code(response_code::application_error);
            res->message("invalid request code");
            break;
    }

    //TODO make this function asynchronous
    res->complete();
    return status::ok;
}

const char* service::execute_statement(std::string_view sql, jogasaki::api::transaction_handle tx)
{
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->create_executable(sql, e); rc != jogasaki::status::ok) {
        return "error in db_->create_executable()";
    }
    if(auto rc = tx->execute(*e); rc != jogasaki::status::ok) {
        return "error in transaction_->execute()";
    }
    return nullptr;
}

void service::set_metadata(std::size_t rid, ::schema::RecordMeta& meta)
{
    auto metadata = cursors_.at(rid).result_set_->meta();
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

void service::release_writers(
    tateyama::api::endpoint::response& res,
    Cursor& cursor
) {
    if (channel_ && cursor.writer_) {
        channel_->release(*cursor.writer_);
        cursor.writer_ = nullptr;
    }
    if (channel_) {
        res.release_channel(*channel_);
    }
}

const char* service::execute_query(
    tateyama::api::endpoint::response& res,
    std::string_view sql,
    std::size_t rid,
    jogasaki::api::transaction_handle tx
) {
    if (!tx) {
        LOG(WARNING) << "transaction begin implicitly";  //TODO stop proceed
        if (auto st = db_->create_transaction(tx); st != jogasaki::status::ok) {
            fail();
        }
    }
    if (cursors_.size() < (rid + 1)) {
        cursors_.resize(rid + 1);
    }

    auto& cursor = cursors_.at(rid);
    cursor.wire_name_ = std::string("resultset-");
    cursor.wire_name_ += std::to_string(rid);
    res.acquire_channel(cursor.wire_name_, channel_);
    channel_->acquire(cursor.writer_);

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->create_executable(sql, e); rc != jogasaki::status::ok) {
        return "error in db_->create_executable()";
    }
    auto& rs = cursor.result_set_;
    if(auto rc = tx->execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        return "error in transaction_->execute()";
    }

    cursor.iterator_ = rs->iterator();
    return nullptr;
}

void service::next(std::size_t rid) {
    auto& cursor = cursors_.at(rid);
    const jogasaki::api::record_meta* meta = cursor.result_set_->meta();
    auto iterator = cursor.result_set_->iterator();
    while(true) {
        auto* rec = iterator->next();
        auto& wrt = *cursor.writer_;
        if (rec != nullptr) {
            for (std::size_t i=0, n=meta->field_count(); i < n; ++i) {
                if (rec->is_null(i)) {
                    msgpack::pack(wrt, msgpack::type::nil_t());
                } else {
                    switch (meta->at(i).kind()) {
                        case jogasaki::api::field_type_kind::int4:
                            msgpack::pack(wrt, rec->get_int4(i)); break;
                        case jogasaki::api::field_type_kind::int8:
                            msgpack::pack(wrt, rec->get_int8(i)); break;
                        case jogasaki::api::field_type_kind::float4:
                            msgpack::pack(wrt, rec->get_float4(i)); break;
                        case jogasaki::api::field_type_kind::float8:
                            msgpack::pack(wrt, rec->get_float8(i)); break;
                        case jogasaki::api::field_type_kind::character:
                            msgpack::pack(wrt, rec->get_character(i)); break;
                        default:
                            fail();
                    }
                }
            }
        } else {
            VLOG(1) << "detect eor" << std::endl;
            wrt.commit();
            break;
        }
    }
}

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

const char* service::execute_prepared_statement(
    std::size_t sid,
    jogasaki::api::parameter_set& params,
    jogasaki::api::transaction_handle tx
) {
    if (!tx) {
        LOG(WARNING) << "transaction begin implicitly";  //TODO stop proceed
        if (auto st = db_->create_transaction(tx); st != jogasaki::status::ok) {
            fail();
        }
    }
    jogasaki::api::statement_handle handle{sid};

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->resolve(handle, params, e); rc != jogasaki::status::ok) {
        return "error in db_->resolve()";
    }
    if(auto rc = tx->execute(*e); rc != jogasaki::status::ok) {
        return "error in transaction_->execute()";
    }
    return nullptr;
}

const char* service::execute_prepared_query(
    tateyama::api::endpoint::response& res,
    std::size_t sid,
    jogasaki::api::parameter_set& params,
    std::size_t rid,
    jogasaki::api::transaction_handle tx
) {
    if (!tx) {
        LOG(WARNING) << "transaction begin implicitly";  //TODO stop proceed
        if (auto st = db_->create_transaction(tx); st != jogasaki::status::ok) {
            fail();
        }
    }
    if (cursors_.size() < (rid + 1)) {
        cursors_.resize(rid + 1);
    }

    auto& cursor = cursors_.at(rid);
    cursor.wire_name_ = std::string("resultset-");
    cursor.wire_name_ += std::to_string(rid);

    res.acquire_channel(cursor.wire_name_, channel_);
    channel_->acquire(cursor.writer_);

    jogasaki::api::statement_handle handle{sid};
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->resolve(handle, params, e); rc != jogasaki::status::ok) {
        return "error in db_->resolve()";
    }

    auto& rs = cursor.result_set_;
    if(auto rc = tx->execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        return "error in transaction_->execute()";
    }

    cursor.iterator_ = rs->iterator();
    return nullptr;
}

void service::reply(response& res, ::response::Response& r) {
    std::stringstream ss{};
    if (!r.SerializeToOstream(&ss)) {
        std::abort();
    }
    res.body(ss.str());
}

}

namespace tateyama::api::endpoint {

std::unique_ptr<service> create_service(jogasaki::api::database& db) {
    return std::make_unique<impl::service>(db);
}

}
