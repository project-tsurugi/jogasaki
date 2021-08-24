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

#include <jogasaki/status.h>
#include <jogasaki/api/database.h>
#include <jogasaki/configuration.h>
#include <jogasaki/api/impl/parameter_set.h>
#include <jogasaki/api/impl/prepared_statement.h>
#include <jogasaki/api/impl/executable_statement.h>

#include <tateyama/api/endpoint/request.h>
#include <tateyama/api/endpoint/response.h>
#include <jogasaki/kvs/database.h>

#include "request.pb.h"
#include "response.pb.h"
#include "common.pb.h"
#include "common.pb.h"

namespace tateyama::api::endpoint::impl {

tateyama::status service::operator()(
    std::shared_ptr<tateyama::api::endpoint::request const> req,
    std::shared_ptr<tateyama::api::endpoint::response> res
) {
    ::request::Request proto_req;
    if (!proto_req.ParseFromString(std::string(req->payload()))) {
        LOG(ERROR) << "parse error" << std::endl;
    } else {
        VLOG(1) << "s:" << proto_req.session_handle().handle() << std::endl;
    }

    switch (proto_req.request_case()) {
        case ::request::Request::RequestCase::kBegin:
            VLOG(1) << "begin" << std::endl;
            {
                if (!transaction_) {
                    if (transaction_ = db_->create_transaction(); transaction_ != nullptr) {
                        ::common::Transaction t;
                        ::response::Begin b;
                        ::response::Response r;

                        t.set_handle(++transaction_id_);
                        b.set_allocated_transaction_handle(&t);
                        r.set_allocated_begin(&b);
                        reply(*res, r);
                        r.release_begin();
                        b.release_transaction_handle();
                    } else {
                        error<::response::Begin>(*res, "error in db_->create_transaction()");
                    }
                } else {
                    error<::response::Begin>(*res, "transaction has already begun");
                }
            }
            break;
        case ::request::Request::RequestCase::kPrepare:
            VLOG(1) << "prepare" << std::endl;
            {
                std::size_t sid = prepared_statements_index_;

                auto pp = proto_req.mutable_prepare();
                auto hvs = pp->mutable_host_variables();
                auto sql = pp->mutable_sql();
                VLOG(1)
                    << *sql
                    << std::endl;
                if (prepared_statements_.size() < (sid + 1)) {
                    prepared_statements_.resize(sid + 1);
                }

                for(std::size_t i = 0; i < static_cast<std::size_t>(hvs->variables_size()) ;i++) {
                    auto hv = hvs->mutable_variables(i);
                    switch(hv->type()) {
                        case ::common::DataType::INT4:
                            db_->register_variable(hv->name(), jogasaki::api::field_type_kind::int4);
                            break;
                        case ::common::DataType::INT8:
                            db_->register_variable(hv->name(), jogasaki::api::field_type_kind::int8);
                            break;
                        case ::common::DataType::FLOAT4:
                            db_->register_variable(hv->name(), jogasaki::api::field_type_kind::float4);
                            break;
                        case ::common::DataType::FLOAT8:
                            db_->register_variable(hv->name(), jogasaki::api::field_type_kind::float8);
                            break;
                        case ::common::DataType::STRING:
                            db_->register_variable(hv->name(), jogasaki::api::field_type_kind::character);
                            break;
                        default:
                            std::abort();
                    }
                }
                if(auto rc = db_->prepare(*sql, prepared_statements_.at(sid)); rc == jogasaki::status::ok) {
                    ::common::PreparedStatement ps;
                    ::response::Prepare p;
                    ::response::Response r;

                    ps.set_handle(sid);
                    p.set_allocated_prepared_statement_handle(&ps);
                    r.set_allocated_prepare(&p);
                    reply(*res, r);
                    r.release_prepare();
                    p.release_prepared_statement_handle();

                    prepared_statements_index_ = sid + 1;
                } else {
                    error<::response::Prepare>(*res, "error in db_->prepare()");
                }
            }
            break;
        case ::request::Request::RequestCase::kExecuteStatement:
            VLOG(1) << "execute_statement" << std::endl;
            {
                auto eq = proto_req.mutable_execute_statement();
                VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle()
                    << *(eq->mutable_sql())
                    << std::endl;
                if (auto err = execute_statement(*(eq->mutable_sql())); err == nullptr) {
                    ::response::Success s;
                    ::response::ResultOnly ok;
                    ::response::Response r;

                    ok.set_allocated_success(&s);
                    r.set_allocated_result_only(&ok);

                    reply(*res, r);
                    r.release_result_only();
                    ok.release_success();
                } else {
                    error<::response::ResultOnly>(*res, err);
                }
            }
            break;
        case ::request::Request::RequestCase::kExecuteQuery:
            VLOG(1) << "execute_query" << std::endl;
            {
                auto eq = proto_req.mutable_execute_query();
                VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle()
                    << *(eq->mutable_sql())
                    << std::endl;

                if (auto err = execute_query(*res, *(eq->mutable_sql()), ++resultset_id_); err == nullptr) {
                    schema::RecordMeta meta;
                    ::response::ResultSetInfo i;
                    ::response::ExecuteQuery e;
                    ::response::Response r;

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
                } else {
                    error<::response::ExecuteQuery>(*res, err);
                }
            }
            break;
        case ::request::Request::RequestCase::kExecutePreparedStatement:
            VLOG(1) << "execute_prepared_statement" << std::endl;
            {
                auto pq = proto_req.mutable_execute_prepared_statement();
                auto ph = pq->mutable_prepared_statement_handle();
                auto sid = ph->handle();
                VLOG(1) << "tx:" << pq->mutable_transaction_handle()->handle()
                    << "sid:" << sid
                    << std::endl;

                auto params = jogasaki::api::create_parameter_set();
                set_params(pq->mutable_parameters(), params);
                if (auto err = execute_prepared_statement(sid, *params); err == nullptr) {
                    ::response::Success s;
                    ::response::ResultOnly ok;
                    ::response::Response r;

                    ok.set_allocated_success(&s);
                    r.set_allocated_result_only(&ok);

                    reply(*res, r);
                    r.release_result_only();
                    ok.release_success();
                } else {
                    error<::response::ResultOnly>(*res, err);
                }
            }
            break;
        case ::request::Request::RequestCase::kExecutePreparedQuery:
            VLOG(1) << "execute_prepared_query" << std::endl;
            {
                auto pq = proto_req.mutable_execute_prepared_query();
                auto ph = pq->mutable_prepared_statement_handle();
                auto sid = ph->handle();
                VLOG(1) << "tx:" << pq->mutable_transaction_handle()->handle()
                    << "sid:" << sid
                    << std::endl;

                auto params = jogasaki::api::create_parameter_set();
                set_params(pq->mutable_parameters(), params);

                if(auto err = execute_prepared_query(*res, sid, *params, ++resultset_id_); err == nullptr) {
                    schema::RecordMeta meta;
                    ::response::ResultSetInfo i;
                    ::response::ExecuteQuery e;
                    ::response::Response r;

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
                } else {
                    error<::response::ExecuteQuery>(*res, err);
                }
            }
            break;
        case ::request::Request::RequestCase::kCommit:
            VLOG(1) << "commit" << std::endl;
            {
                if (transaction_) {
                    if(auto rc = transaction_->commit(); rc == jogasaki::status::ok) {
                        auto eq = proto_req.mutable_commit();
                        VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle()
                            << std::endl;

                        ::response::Success s;
                        ::response::ResultOnly ok;
                        ::response::Response r;

                        ok.set_allocated_success(&s);
                        r.set_allocated_result_only(&ok);

                        reply(*res, r);
                        r.release_result_only();
                        ok.release_success();

                        transaction_ = nullptr;
                    } else {
                        error<::response::ResultOnly>(*res, "error in transaction_->commit()");
                    }
                } else {
                    error<::response::ResultOnly>(*res, "transaction has not begun");
                }
            }
            break;
        case ::request::Request::RequestCase::kRollback:
            VLOG(1) << "rollback" << std::endl;
            {
                if (transaction_) {
                    if(auto rc = transaction_->abort(); rc == jogasaki::status::ok) {
                        auto eq = proto_req.mutable_rollback();
                        VLOG(1) << "tx:" << eq->mutable_transaction_handle()->handle() << std::endl;

                        ::response::Success s;
                        ::response::ResultOnly ok;
                        ::response::Response r;

                        ok.set_allocated_success(&s);
                        r.set_allocated_result_only(&ok);

                        reply(*res, r);
                        r.release_result_only();
                        ok.release_success();

                        transaction_ = nullptr;
                    } else {
                        error<::response::ResultOnly>(*res, "error in transaction_->abort()");
                    }
                } else {
                    error<::response::ResultOnly>(*res, "transaction has not begun");
                }
            }
            break;
        case ::request::Request::RequestCase::kDisposePreparedStatement:
            VLOG(1) << "dispose_prepared_statement" << std::endl;
            {
                auto dp = proto_req.mutable_dispose_prepared_statement();
                auto ph = dp->mutable_prepared_statement_handle();
                auto sid = ph->handle();

                VLOG(1)
                    << "ps:" << sid
                    << std::endl;

                if(prepared_statements_.size() > sid) {
                    if(prepared_statements_.at(sid)) {
                        prepared_statements_.at(sid) = nullptr;

                        ::response::Success s;
                        ::response::ResultOnly ok;
                        ::response::Response r;

                        ok.set_allocated_success(&s);
                        r.set_allocated_result_only(&ok);

                        reply(*res, r);
                        r.release_result_only();
                        ok.release_success();
                    } else {
                        error<::response::ResultOnly>(*res, "cannot find prepared statement with the index given");
                    }
                } else {
                    error<::response::ResultOnly>(*res, "index is larger than the number of prepred statment registerd");
                }
            }
            break;
        case ::request::Request::RequestCase::kDisconnect:
            VLOG(1) << "disconnect" << std::endl;
            {
                ::response::Success s;
                ::response::ResultOnly ok;
                ::response::Response r;

                ok.set_allocated_success(&s);
                r.set_allocated_result_only(&ok);

                reply(*res, r);
                r.release_result_only();
                ok.release_success();
            }
            break;
        case ::request::Request::RequestCase::REQUEST_NOT_SET:
            VLOG(1) << "not used" << std::endl;
            break;
        default:
            LOG(ERROR) << "????" << std::endl;
            break;
    }
    return status::ok;
}

const char* service::execute_statement(std::string_view sql)
{
    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->create_executable(sql, e); rc != jogasaki::status::ok) {
        return "error in db_->create_executable()";
    }
    if(auto rc = transaction_->execute(*e); rc != jogasaki::status::ok) {
        return "error in transaction_->execute()";
    }
    return nullptr;
}

void service::set_metadata(std::size_t rid, schema::RecordMeta& meta)
{
    auto metadata = cursors_.at(rid).result_set_->meta();
    std::size_t n = metadata->field_count();

    for (std::size_t i = 0; i < n; i++) {
        auto column = std::make_unique<schema::RecordMeta_Column>();
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
                column->set_type(::common::DataType::STRING);
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

const char* service::execute_query(
    tateyama::api::endpoint::response& res,
    std::string_view sql,
    std::size_t rid
) {
    if (!transaction_) {
        transaction_ = db_->create_transaction();
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
    if(auto rc = transaction_->execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        return "error in transaction_->execute()";
    }

    cursor.iterator_ = rs->iterator();
    return nullptr;
}

void service::next(std::size_t rid) {
    auto& cursor = cursors_.at(rid);
//    tsubakuro::common::wire::server_wire_container::resultset_wire_container& wire = *cursor.resultset_wire_container_;
    const jogasaki::api::record_meta* meta = cursor.result_set_->meta();
    auto iterator = cursor.result_set_->iterator();
    while(true) {
        auto record = iterator->next();
        if (record != nullptr) {
            for (std::size_t cindex = 0; cindex < meta->field_count(); cindex++) {
                if (record->is_null(cindex)) {
                    msgpack::pack(*cursor.writer_, msgpack::type::nil_t());
                } else {
                    switch (meta->at(cindex).kind()) {
                        case jogasaki::api::field_type_kind::int4:
                            msgpack::pack(*cursor.writer_, record->get_int4(cindex)); break;
                        case jogasaki::api::field_type_kind::int8:
                            msgpack::pack(*cursor.writer_, record->get_int8(cindex)); break;
                        case jogasaki::api::field_type_kind::float4:
                            msgpack::pack(*cursor.writer_, record->get_float4(cindex)); break;
                        case jogasaki::api::field_type_kind::float8:
                            msgpack::pack(*cursor.writer_, record->get_float8(cindex)); break;
                        case jogasaki::api::field_type_kind::character:
                            msgpack::pack(*cursor.writer_, record->get_character(cindex)); break;
                        default:
                            std::cerr << "type undefined" << std::endl; break;
                    }
                }
            }
        } else {
            VLOG(1) << "detect eor" << std::endl;
            cursor.writer_->commit();
            break;
        }
    }
}

void service::set_params(::request::ParameterSet* ps, std::unique_ptr<jogasaki::api::parameter_set>& params)
{
    for (std::size_t i = 0; i < static_cast<std::size_t>(ps->parameters_size()) ;i++) {
        auto p = ps->mutable_parameters(i);
        switch (p->value_case()) {
            case ::request::ParameterSet::Parameter::ValueCase::kIValue:
                params->set_int4(p->name(), p->l_value());
                break;
            case ::request::ParameterSet::Parameter::ValueCase::kLValue:
                params->set_int8(p->name(), p->l_value());
                break;
            case ::request::ParameterSet::Parameter::ValueCase::kFValue:
                params->set_float4(p->name(), p->l_value());
                break;
            case ::request::ParameterSet::Parameter::ValueCase::kDValue:
                params->set_float8(p->name(), p->l_value());
                break;
            case ::request::ParameterSet::Parameter::ValueCase::kSValue:
                params->set_character(p->name(), p->s_value());
                break;
            default:
                std::cerr << "type undefined" << std::endl;
                std::abort();
                break;
        }
    }
}

const char* service::execute_prepared_statement(std::size_t sid, jogasaki::api::parameter_set& params)
{
    if (!transaction_) {
        transaction_ = db_->create_transaction();
    }

    std::unique_ptr<jogasaki::api::executable_statement> e{};
    if(auto rc = db_->resolve(*prepared_statements_.at(sid), params, e); rc != jogasaki::status::ok) {
        return "error in db_->resolve()";
    }
    if(auto rc = transaction_->execute(*e); rc != jogasaki::status::ok) {
        return "error in transaction_->execute()";
    }
    return nullptr;
}

const char* service::execute_prepared_query(
    tateyama::api::endpoint::response& res,
    std::size_t sid,
    jogasaki::api::parameter_set& params,
    std::size_t rid
) {
    if (!transaction_) {
        transaction_ = db_->create_transaction();
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
    if(auto rc = db_->resolve(*prepared_statements_.at(sid), params, e); rc != jogasaki::status::ok) {
        return "error in db_->resolve()";
    }

    auto& rs = cursor.result_set_;
    if(auto rc = transaction_->execute(*e, rs); rc != jogasaki::status::ok || !rs) {
        return "error in transaction_->execute()";
    }

    cursor.iterator_ = rs->iterator();
    return nullptr;
}

/*
void service::deploy_metadata(std::size_t table_id)
{
    manager::metadata::ErrorCode error;

    auto datatypes = std::make_unique<manager::metadata::DataTypes>(FLAGS_dbname);
    error = datatypes->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        //        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        return;
    }
    auto tables = std::make_unique<manager::metadata::Tables>(FLAGS_dbname);
    error = tables->Metadata::load();
    if (error != manager::metadata::ErrorCode::OK) {
        //        channel_->send_ack(ERROR_CODE::FILE_IO_ERROR);
        return;
    }

    boost::property_tree::ptree table;
    if ((error = tables->get(table_id, table)) == manager::metadata::ErrorCode::OK) {

        // table metadata
        auto id = table.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Tables::ID);
        auto table_name = table.get_optional<std::string>(manager::metadata::Tables::NAME);
        if (!id || !table_name || (id.value() != table_id)) {
            //            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            return;
        }

        boost::property_tree::ptree primary_keys = table.get_child(manager::metadata::Tables::PRIMARY_KEY_NODE);

        std::vector<std::size_t> pk_columns;  // index: received order, content: ordinalPosition
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, primary_keys) {
            const boost::property_tree::ptree& value = node.second;
            boost::optional<uint64_t> primary_key = value.get_value_optional<uint64_t>();
            pk_columns.emplace_back(primary_key.value());
        }
        if(pk_columns.empty()) {
            //            channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
            return;
        }

        // column metadata
        std::map<std::size_t, const boost::property_tree::ptree*> columns_map;          // key: Column.ordinalPosition
        BOOST_FOREACH (const boost::property_tree::ptree::value_type& node, table.get_child(manager::metadata::Tables::COLUMNS_NODE)) {
            auto ordinal_position = node.second.get_optional<uint64_t>(manager::metadata::Tables::Column::ORDINAL_POSITION);
            if (!ordinal_position) {
                //                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                return;
            }
            columns_map[ordinal_position.value()] = &node.second;
        }

        takatori::util::reference_vector<yugawara::storage::column> columns;            // index: ordinalPosition order (the first value of index and order are different)
        std::vector<std::size_t> value_columns;                                         // index: received order, content: ordinalPosition of the column
        std::vector<bool> is_descendant;                                                // index: ordinalPosition order (the first value of index and order are different)
        std::size_t ordinal_position_value = 1;
        for(auto &&e : columns_map) {
            if (ordinal_position_value != e.first) {
                //                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                return;
            }
            const boost::property_tree::ptree& column = *e.second;

            auto nullable = column.get_optional<bool>(manager::metadata::Tables::Column::NULLABLE);
            auto data_type_id = column.get_optional<manager::metadata::ObjectIdType>(manager::metadata::Tables::Column::DATA_TYPE_ID);
            auto name = column.get_optional<std::string>(manager::metadata::Tables::Column::NAME);
            auto ordinal_position = column.get_optional<uint64_t>(manager::metadata::Tables::Column::ORDINAL_POSITION);
            if (!nullable || !data_type_id || !name) {
                //                channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                return;
            }
            auto nullable_value = nullable.value();
            auto data_type_id_value = static_cast<manager::metadata::DataTypes::DataTypesId>(data_type_id.value());
            auto name_value = name.value();

            if (std::vector<std::size_t>::iterator itr = std::find(pk_columns.begin(), pk_columns.end(), ordinal_position_value); itr != pk_columns.end()) {  // is this pk_column ?
                if(nullable_value) {
                    //                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);  // pk_column must not be nullable
                    return;
                }
            } else {  // this is value column
                value_columns.emplace_back(ordinal_position_value);
            }

            bool d{false};
            auto direction = column.get_optional<uint64_t>(manager::metadata::Tables::Column::DIRECTION);
            if (direction) {
                d = static_cast<manager::metadata::Tables::Column::Direction>(direction.value()) == manager::metadata::Tables::Column::Direction::DESCENDANT;
            }
            is_descendant.emplace_back(d);                              // is_descendant will be used in PK section

            switch(data_type_id_value) {  // build yugawara::storage::column
                case manager::metadata::DataTypes::DataTypesId::INT32:
                    columns.emplace_back(yugawara::storage::column(name_value, takatori::type::int4(), yugawara::variable::nullity(nullable_value)));
                    break;
                case manager::metadata::DataTypes::DataTypesId::INT64:
                    columns.emplace_back(yugawara::storage::column(name_value, takatori::type::int8(), yugawara::variable::nullity(nullable_value)));
                    break;
                case manager::metadata::DataTypes::DataTypesId::FLOAT32:
                    columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float4(), yugawara::variable::nullity(nullable_value)));
                    break;
                case manager::metadata::DataTypes::DataTypesId::FLOAT64:
                    columns.emplace_back(yugawara::storage::column(name_value, takatori::type::float8(), yugawara::variable::nullity(nullable_value)));
                    break;
                case manager::metadata::DataTypes::DataTypesId::CHAR:
                case manager::metadata::DataTypes::DataTypesId::VARCHAR:
                {
                    std::size_t data_length_value{1};  // for CHAR
                    auto varying = column.get_optional<bool>(manager::metadata::Tables::Column::VARYING);
                    if(!varying) {  // varying field is necessary for CHAR/VARCHRA
                        //                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                        return;
                    }
                    auto varying_value = varying.value();
                    if((!varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::CHAR)) ||
                        (varying_value && (data_type_id_value != manager::metadata::DataTypes::DataTypesId::VARCHAR))) {
                        //                    channel_->send_ack(ERROR_CODE::INVALID_PARAMETER);
                        return;
                    }
                    auto data_length = column.get_optional<uint64_t>(manager::metadata::Tables::Column::DATA_LENGTH);
                    if (!data_length) {
                        if(varying_value) {  // data_length field is necessary for VARCHAR
                            //                        channel_->send_ack(ERROR_CODE::UNSUPPORTED);
                            return;
                        }
                    } else {
                        data_length_value = data_length.value();
                    }
                    columns.emplace_back(yugawara::storage::column(name_value,
                        takatori::type::character(takatori::type::varying_t(varying_value), data_length_value),
                        yugawara::variable::nullity(nullable_value)));
                    break;
                }
                default:
                    std::abort();  // FIXME
            }
            ordinal_position_value++;
        }

        auto t = std::make_shared<yugawara::storage::table>(yugawara::storage::table::simple_name_type(table_name.value()), std::move(columns));
        if (auto rc = db_->create_table(t); rc != jogasaki::status::ok) {
            //            channel_->send_ack((rc == jogasaki::status::err_already_exists) ? ERROR_CODE::INVALID_PARAMETER : ERROR_CODE::UNKNOWN);
            return;
        }

        // build key metadata (yugawara::storage::index::key)
        std::vector<yugawara::storage::index::key> keys;
        for (std::size_t position : pk_columns) {
            auto sort_direction = is_descendant[position-1] ? takatori::relation::sort_direction::descendant : takatori::relation::sort_direction::ascendant;
            keys.emplace_back(yugawara::storage::index::key(t->columns()[position-1], sort_direction));
        }

        // build value metadata (yugawara::storage::index::column_ref)
        std::vector<yugawara::storage::index::column_ref> values;
        for(std::size_t position : value_columns) {
            values.emplace_back(yugawara::storage::index::column_ref(t->columns()[position-1]));
        }

        auto i = std::make_shared<yugawara::storage::index>(
            t,
            yugawara::storage::index::simple_name_type(table_name.value()),
            std::move(keys),
            std::move(values),
            yugawara::storage::index_feature_set{
                ::yugawara::storage::index_feature::find,
                ::yugawara::storage::index_feature::scan,
                ::yugawara::storage::index_feature::unique,
                ::yugawara::storage::index_feature::primary,
                }
                );
        if(db_->create_index(i) != jogasaki::status::ok) {
            //            channel_->send_ack(ERROR_CODE::UNKNOWN);
            return;
        }

        //        channel_->send_ack(ERROR_CODE::OK);
    } else {
        //        channel_->send_ack(ERROR_CODE::UNKNOWN);
    }
}
 */

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
